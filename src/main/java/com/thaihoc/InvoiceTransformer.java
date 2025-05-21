package com.thaihoc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.UUID;

public class InvoiceTransformer extends RichFlatMapFunction<String, InvoiceMysqlRecord> {
    private transient ObjectMapper objectMapper;
    private final int maxGroupIdValue;
    public InvoiceTransformer(int maxGroupIdValue) {
        this.maxGroupIdValue = maxGroupIdValue;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        objectMapper = new ObjectMapper();
    }

    @Override
    public void flatMap(String jsonString, Collector<InvoiceMysqlRecord> out) throws Exception {
        try {
            JsonNode rootNode = objectMapper.readTree(jsonString);
            JsonNode invoicesBatchNode = rootNode.path("inv_pack");

            if (invoicesBatchNode.isArray()) {
                for (int i = 0;i < invoicesBatchNode.size();i++) {
                    JsonNode singleInvoiceJson = invoicesBatchNode.path(i);
                    InvoiceMysqlRecord record = new InvoiceMysqlRecord();
                    JsonNode invNode = singleInvoiceJson.path("inv");

                    record.inv = objectMapper.writeValueAsString(singleInvoiceJson);
                    record.api_type = (byte) singleInvoiceJson.path("api_type").asInt();
                    record.tax_schema = "0306182043";
                    record.fpt_einvoice_res_code = null;
                    record.fpt_einvoice_res_msg = null;
                    record.fpt_einvoice_res_json = null;
                    record.retry = 0;
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

                    out.collect(record);
                }
            }
        } catch (Exception e) {
            System.err.println("json: " + jsonString + " - err: " + e.getMessage());
        }
    }
}
