package com.thaihoc.process.response;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thaihoc.model.InvoiceResponsePacket;
import com.thaihoc.model.RecordInterface;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

public class InvoiceResponseKafkaRouter {
    // Define output tags for different API types (Kafka outputs)
    public static final OutputTag<String> CRT_OUTPUT_TAG = new OutputTag<String>("crt-response-batch") {};
    public static final OutputTag<String> UPD_OUTPUT_TAG = new OutputTag<String>("upd-response-batch") {};
    public static final OutputTag<String> DEL_OUTPUT_TAG = new OutputTag<String>("del-response-batch") {};
    public static final OutputTag<String> REP_OUTPUT_TAG = new OutputTag<String>("rep-response-batch") {};
    public static final OutputTag<String> ADJ_OUTPUT_TAG = new OutputTag<String>("adj-response-batch") {};

    private final ObjectMapper objectMapper;
    private final InvoiceResponseItemFactory itemFactory;

    public InvoiceResponseKafkaRouter(ObjectMapper objectMapper, InvoiceResponseItemFactory itemFactory) {
        this.objectMapper = objectMapper;
        this.itemFactory = itemFactory;
    }

    public void routeKafkaBatch(List<RecordInterface> records, 
                               KeyedProcessFunction<Byte, RecordInterface, String>.Context ctx) throws Exception {
        if (records.isEmpty()) return;
        
        // Get api_type from first record to determine routing
        byte apiType = records.get(0).getApiType();
        
        // Create batch response
        List<InvoiceResponsePacket.InvoiceResponseItem> items = new ArrayList<>();
        
        for (RecordInterface record : records) {
            InvoiceResponsePacket.InvoiceResponseItem item = itemFactory.createResponseItem(record);
            if (item != null) {
                items.add(item);
            }
        }
        
        if (!items.isEmpty()) {
            InvoiceResponsePacket packet = new InvoiceResponsePacket();
            packet.inv_pack_res = items;
            
            String jsonResponse = objectMapper.writeValueAsString(packet);
            
            // Route to appropriate side output based on api_type
            switch (apiType) {
                case 10:
                    ctx.output(CRT_OUTPUT_TAG, jsonResponse);
                    break;
                case 11:
                    ctx.output(UPD_OUTPUT_TAG, jsonResponse);
                    break;
                case 12:
                    ctx.output(DEL_OUTPUT_TAG, jsonResponse);
                    break;
                case 13:
                    ctx.output(REP_OUTPUT_TAG, jsonResponse);
                    break;
                case 14:
                    ctx.output(ADJ_OUTPUT_TAG, jsonResponse);
                    break;
                default:
                    System.err.println("Unknown api_type: " + apiType + ". Skipping batch.");
            }
        }
    }
} 