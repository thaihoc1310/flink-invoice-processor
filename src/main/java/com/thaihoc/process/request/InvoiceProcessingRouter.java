package com.thaihoc.process.request;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.thaihoc.model.InvoiceMysqlRecord;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.configuration.Configuration;
import java.util.List;

public class InvoiceProcessingRouter extends ProcessFunction<String, InvoiceMysqlRecord> {
    private final int maxGroupIdValue;
    private final int maxRetriesConfig;

    private final OutputTag<String> retryOutput;
    private final OutputTag<String> dlqOutput;

    private transient InvoiceTransformer transformer;
    private transient ObjectMapper objectMapper;

    public InvoiceProcessingRouter(int maxGroupIdValue, int maxRetriesConfig,
                                   OutputTag<String> retryOutput, OutputTag<String> dlqOutput) {
        this.maxGroupIdValue = maxGroupIdValue;
        this.maxRetriesConfig = maxRetriesConfig;
        this.retryOutput = retryOutput;
        this.dlqOutput = dlqOutput;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        transformer = new InvoiceTransformer(maxGroupIdValue);
        objectMapper = new ObjectMapper();
    }

    @Override
    public void processElement(String incomingJsonString, Context ctx, Collector<InvoiceMysqlRecord> out) throws JsonProcessingException {
        String actualPayloadJson = incomingJsonString;
        int currentAttempt = 0;
        JsonNode rootNode = objectMapper.readTree(incomingJsonString);
        if (rootNode.has("payload") && rootNode.has("attempt")) {
            JsonNode payloadNode = rootNode.get("payload");
            if (payloadNode.isObject()) {
                actualPayloadJson = objectMapper.writeValueAsString(payloadNode);
            } else {
                actualPayloadJson = payloadNode.asText();
            }
            currentAttempt = rootNode.get("attempt").asInt();
        }

        try {
            List<InvoiceMysqlRecord> records = transformer.transform(actualPayloadJson);
            for (InvoiceMysqlRecord record : records) {
                record.retry = (byte) currentAttempt;
                out.collect(record);
            }
        } catch (Exception e) {
            System.err.println("ERROR: Failed to process message (attempt " + currentAttempt + " of " + maxRetriesConfig + "): "
            + e.getMessage() + ". Routing for next attempt or DLQ. Message: " + actualPayloadJson.substring(0, Math.min(actualPayloadJson.length(), 200)) + "...");
            
            int nextAttempt = currentAttempt + 1;

            if (nextAttempt < maxRetriesConfig) {
                try {
                    ObjectNode retryWrapper = objectMapper.createObjectNode();
                    try {
                        JsonNode payloadJsonNode = objectMapper.readTree(actualPayloadJson);
                        retryWrapper.set("payload", payloadJsonNode);
                    } catch (JsonProcessingException jsonEx) {
                        retryWrapper.put("payload", actualPayloadJson);
                    }
                    retryWrapper.put("attempt", nextAttempt);
                    ctx.output(retryOutput, objectMapper.writeValueAsString(retryWrapper));
                } catch (JsonProcessingException jsonEx) {
                    System.err.println("CRITICAL: Error creating retry wrapper, sending original payload to DLQ: " + jsonEx.getMessage());
                    ctx.output(dlqOutput, actualPayloadJson);
                }
            } else {
                System.out.println("INFO: Max retries (" + maxRetriesConfig + ") reached for message. Sending to DLQ: " + actualPayloadJson.substring(0, Math.min(actualPayloadJson.length(), 200)) + "...");
                ctx.output(dlqOutput, actualPayloadJson);
            }
        }
    }
}