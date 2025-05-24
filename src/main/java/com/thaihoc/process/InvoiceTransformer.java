package com.thaihoc.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thaihoc.model.InvoiceMysqlRecord;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class InvoiceTransformer{
    private final transient ObjectMapper objectMapper;
    private final int maxGroupIdValue;

    public InvoiceTransformer(int maxGroupIdValue) {
        this.maxGroupIdValue = maxGroupIdValue;
        this.objectMapper = new ObjectMapper();
    }

    public List<InvoiceMysqlRecord> transform(String jsonString) throws Exception {
        List<InvoiceMysqlRecord> records = new ArrayList<>();
        JsonNode rootNode = objectMapper.readTree(jsonString);
        JsonNode invoicesBatchNode = rootNode.path("inv_pack");

            if (invoicesBatchNode.isArray()) {
                for (int i = 0;i < invoicesBatchNode.size();i++) {
                    JsonNode singleInvoiceJson = invoicesBatchNode.path(i);
                    InvoiceMysqlRecord record = new InvoiceMysqlRecord();
                    JsonNode invNode;
                    boolean hasInvNode = singleInvoiceJson.has("inv");

                    if (hasInvNode) {
                        invNode = singleInvoiceJson.get("inv");
                        record.tax_schema = invNode.get("stax").asText();
                        record.sid = invNode.get("sid").asText();
                    } else {
                        record.tax_schema = singleInvoiceJson.get("stax").asText();
                        record.sid = singleInvoiceJson.get("sid").asText();
                    }

                    record.inv = objectMapper.writeValueAsString(singleInvoiceJson);
                    record.api_type = (byte) singleInvoiceJson.get("api_type").asInt();
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

                    record.syncid = UUID.randomUUID().toString();

                    records.add(record);
                }
            }
        return records;
    }
}
