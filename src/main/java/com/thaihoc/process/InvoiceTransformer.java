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
        JsonNode rootNode = objectMapper.readTree(jsonString); // Can throw JsonProcessingException
        JsonNode invoicesBatchNode = rootNode.path("inv_pack");

            if (invoicesBatchNode.isArray()) {
                for (int i = 0;i < invoicesBatchNode.size();i++) {
                    JsonNode singleInvoiceJson = invoicesBatchNode.path(i);
                    InvoiceMysqlRecord record = new InvoiceMysqlRecord();
                    JsonNode invNode = singleInvoiceJson.path("inv");
                    if (invNode.isMissingNode() || invNode.path("sid").isMissingNode()){
                        // Example of a business validation that might cause an exception
                        throw new IllegalArgumentException("Missing 'inv' or 'inv.sid' in invoice item: " + singleInvoiceJson);
                    }
                    record.inv = objectMapper.writeValueAsString(singleInvoiceJson);
                    record.api_type = (byte) singleInvoiceJson.path("api_type").asInt();
                    record.tax_schema = "0306182043";
                    record.fpt_einvoice_res_code = null;
                    record.fpt_einvoice_res_msg = null;
                    record.fpt_einvoice_res_json = null;
                    record.state = 1;
                    record.group_id = (byte) (i % maxGroupIdValue);
                    record.created_date = new Timestamp(System.currentTimeMillis());
                    record.updated_date = null;
                    record.callback_res_code = null;
                    record.callback_res_msg = null;
                    record.callback_res_json = null;
                    record.process_kafka = null;

                    record.sid = invNode.path("sid").asText();
                    record.syncid = UUID.randomUUID().toString();

                    records.add(record);
                }
            }
        return records;
    }
}
