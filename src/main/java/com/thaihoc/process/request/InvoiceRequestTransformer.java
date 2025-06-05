package com.thaihoc.process.request;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thaihoc.model.request.InvoiceMysqlRecord;
import com.thaihoc.model.retry.InvoiceRetryRecord;
import org.apache.flink.util.OutputTag;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;

public class InvoiceRequestTransformer {
    private final transient ObjectMapper objectMapper;
    private final int maxGroupIdValue;
    private final long retryIntervalMs;

    public static final OutputTag<InvoiceRetryRecord> CREATE_RETRY_TAG = new OutputTag<>("create-retry") {
    };
    public static final OutputTag<InvoiceRetryRecord> UPDATE_RETRY_TAG = new OutputTag<>("update-retry") {
    };
    public static final OutputTag<InvoiceRetryRecord> MAX_RETRY_TAG = new OutputTag<>("max-retry") {
    };
    public static final OutputTag<InvoiceRetryRecord> DELETE_RETRY_TAG = new OutputTag<>("delete-retry") {
    };

    public InvoiceRequestTransformer(int maxGroupIdValue, long retryIntervalMs) {
        this.maxGroupIdValue = maxGroupIdValue;
        this.objectMapper = new ObjectMapper();
        this.retryIntervalMs = retryIntervalMs;
    }

    public List<InvoiceMysqlRecord> transform(String jsonString, Context ctx) throws Exception {
        List<InvoiceMysqlRecord> records = new ArrayList<>();
        JsonNode rootNode = objectMapper.readTree(jsonString);
        JsonNode invoicesBatchNode = rootNode.path("inv_pack");
        if (invoicesBatchNode.isArray()) {
            for (int i = 0;i < invoicesBatchNode.size();i++) {
                JsonNode singleInvoiceJson = invoicesBatchNode.path(i);
                try {
                    InvoiceMysqlRecord record = transformInvoice(singleInvoiceJson, i);
                    records.add(record);
                } catch (Exception e) {
                    InvoiceRetryRecord retryRecord = createRetryRecord(singleInvoiceJson.toString(), e, getSidFromJson(singleInvoiceJson), getSyncidFromJson(singleInvoiceJson));
                    retryRecord.tag = "CREATE";
                    retryRecord.next_retry_time = retryIntervalMs/1000;
                    ctx.output(CREATE_RETRY_TAG, retryRecord);
                }
            }
        }
        return records;
    }

    public InvoiceMysqlRecord transformInvoice(JsonNode singleInvoiceJson, int i) throws Exception {
        InvoiceMysqlRecord record = new InvoiceMysqlRecord();
        boolean hasInvNode = singleInvoiceJson.has("inv");
        JsonNode invNode = null;

        if (hasInvNode) {
            invNode = singleInvoiceJson.get("inv");
            if (invNode.has("stax")) {
                record.tax_schema = invNode.get("stax").asText();
            } else {
                throw new Exception("stax is null");
            }
        } else {
            record.tax_schema = singleInvoiceJson.get("stax").asText();
        }

        if (singleInvoiceJson.has("sid")) {
            record.sid = singleInvoiceJson.get("sid").asText();
        }else if (hasInvNode && invNode.has("sid")) {
            record.sid = invNode.get("sid").asText();
        }

        if(record.sid == null || record.sid.isEmpty()) {
            throw new Exception("sid is null");
        }

        if (singleInvoiceJson.has("syncid")) {
            record.syncid = singleInvoiceJson.get("syncid").asText();
        }else if(hasInvNode && invNode.has("syncid")) {
            record.syncid = invNode.get("syncid").asText();
        }

        if(record.syncid == null || record.syncid.isEmpty()) {
            record.syncid = UUID.randomUUID().toString();
        }

        record.inv = objectMapper.writeValueAsString(singleInvoiceJson);
        if(singleInvoiceJson.has("api_type")) {
            record.api_type = (byte) singleInvoiceJson.get("api_type").asInt();
        }else {
            throw new Exception("api_type is null");
        }
        record.fpt_einvoice_res_code = null;
        record.fpt_einvoice_res_msg = null;
        record.fpt_einvoice_res_json = null;
        record.state = 0;
        record.group_id = (byte) (i % maxGroupIdValue);
        record.created_date = new Timestamp(System.currentTimeMillis());
        record.updated_date = null;
        record.callback_res_code = null;
        record.callback_res_msg = null;
        record.callback_res_json = null;
        record.process_kafka = null;
        
        return record;
    }


    public InvoiceMysqlRecord transformRetryRecord(InvoiceRetryRecord retryRecord, Context ctx, int maxRetriesConfig) throws Exception {
        if (retryRecord.retry_count > maxRetriesConfig) {
            retryRecord.tag = "MAX_RETRY";
            ctx.output(MAX_RETRY_TAG, retryRecord);
            return null;
        }
        String jsonString = retryRecord.payload;
        JsonNode rootNode = objectMapper.readTree(jsonString);
        try {
            InvoiceMysqlRecord record = transformInvoice(rootNode, retryRecord.retry_count);
            record.retry = retryRecord.retry_count;
            retryRecord.tag = "DELETE";
            ctx.output(DELETE_RETRY_TAG, retryRecord);
            return record;
        } catch (Exception e) {
            retryRecord.retry_count++;
            retryRecord.error_message = e.getMessage();
            retryRecord.error_code = e.getClass().getSimpleName();
            retryRecord.tag = "UPDATE";
            retryRecord.next_retry_time =  retryIntervalMs/1000 * (long)Math.pow(2, retryRecord.retry_count);
            ctx.output(UPDATE_RETRY_TAG, retryRecord);
            return null;
        }
    }


    private String getSidFromJson(JsonNode singleInvoiceJson) {
        if (singleInvoiceJson.has("sid")) {
            return singleInvoiceJson.get("sid").asText();
        }else if (singleInvoiceJson.has("inv")) {
            return singleInvoiceJson.get("inv").get("sid").asText();
        }
        return null;
    }

    private String getSyncidFromJson(JsonNode singleInvoiceJson) {
        if (singleInvoiceJson.has("syncid")) {
            return singleInvoiceJson.get("syncid").asText();
        }else if (singleInvoiceJson.has("inv")) {
            return singleInvoiceJson.get("inv").get("syncid").asText();
        }
        return null;
    }

    private InvoiceRetryRecord createRetryRecord(String jsonString, Exception exception, String sid, String syncid) {
        InvoiceRetryRecord retryRecord = new InvoiceRetryRecord();
        retryRecord.payload = jsonString;
        retryRecord.state = "PENDING";
        retryRecord.job = "REQUEST";
        retryRecord.error_message = exception.getMessage();
        retryRecord.sid = sid;
        retryRecord.syncid = syncid;
        retryRecord.retry_count = 0;
        retryRecord.error_code = exception.getClass().getSimpleName();
        return retryRecord;
    }
        
}
