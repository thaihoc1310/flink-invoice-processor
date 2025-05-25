package com.thaihoc.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thaihoc.model.AsyncInvInRecord;
import com.thaihoc.model.AsyncInvOutRecord;
import com.thaihoc.model.InvoiceResponsePacket;
import com.thaihoc.model.RecordInterface;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

public class BatchResponseProcessor extends KeyedProcessFunction<Byte, RecordInterface, String> {
    private transient ObjectMapper objectMapper;
    private transient ListState<RecordInterface> recordsBuffer;
    private final int batchSize;
    private final long batchTimeoutMs;
    
    // Define output tags for different API types
    public static final OutputTag<String> CRT_OUTPUT_TAG = new OutputTag<String>("crt-response-batch") {};
    public static final OutputTag<String> UPD_OUTPUT_TAG = new OutputTag<String>("upd-response-batch") {};
    public static final OutputTag<String> DEL_OUTPUT_TAG = new OutputTag<String>("del-response-batch") {};
    public static final OutputTag<String> REP_OUTPUT_TAG = new OutputTag<String>("rep-response-batch") {};
    public static final OutputTag<String> ADJ_OUTPUT_TAG = new OutputTag<String>("adj-response-batch") {};

    public BatchResponseProcessor(int batchSize, long batchTimeoutMs) {
        this.batchSize = batchSize;
        this.batchTimeoutMs = batchTimeoutMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        objectMapper = new ObjectMapper();
        
        ListStateDescriptor<RecordInterface> descriptor = new ListStateDescriptor<>(
                "recordsBuffer",
                TypeInformation.of(new TypeHint<RecordInterface>() {})
        );
        recordsBuffer = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(RecordInterface record, Context ctx, Collector<String> out) throws Exception {
        // Add record to buffer
        recordsBuffer.add(record);
        
        // Get current buffer size
        List<RecordInterface> currentBuffer = new ArrayList<>();
        for (RecordInterface bufferedRecord : recordsBuffer.get()) {
            currentBuffer.add(bufferedRecord);
        }
        
        // Check if batch is full
        if (currentBuffer.size() >= batchSize) {
            processBatch(currentBuffer, ctx);
            recordsBuffer.clear();
        } else {
            // Set timer for batch timeout
            long currentTime = ctx.timestamp() != null ? ctx.timestamp() : System.currentTimeMillis();
            ctx.timerService().registerProcessingTimeTimer(currentTime + batchTimeoutMs);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // Process remaining records when timer fires
        List<RecordInterface> currentBuffer = new ArrayList<>();
        for (RecordInterface bufferedRecord : recordsBuffer.get()) {
            currentBuffer.add(bufferedRecord);
        }
        
        if (!currentBuffer.isEmpty()) {
            processBatch(currentBuffer, ctx);
            recordsBuffer.clear();
        }
    }

    private void processBatch(List<RecordInterface> records, Context ctx) throws Exception {
        if (records.isEmpty()) return;
        
        // Get api_type from first record to determine routing
        byte apiType = records.get(0).getApiType();
        
        // Create batch response
        List<InvoiceResponsePacket.InvoiceResponseItem> items = new ArrayList<>();
        
        for (RecordInterface record : records) {
            InvoiceResponsePacket.InvoiceResponseItem item = createResponseItem(record);
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

    private InvoiceResponsePacket.InvoiceResponseItem createResponseItem(RecordInterface record) throws Exception {
        if (record instanceof AsyncInvInRecord) {
            return createInvInResponseItem((AsyncInvInRecord) record);
        } else if (record instanceof AsyncInvOutRecord) {
            return createInvOutResponseItem((AsyncInvOutRecord) record);
        }
        return null;
    }

    private InvoiceResponsePacket.InvoiceResponseItem createInvInResponseItem(AsyncInvInRecord record) throws Exception {
        InvoiceResponsePacket.InvoiceResponseItem item = new InvoiceResponsePacket.InvoiceResponseItem();
        item.sid = record.getSid();
        item.sync_sid = record.getSyncid();
        item.res_code = record.fpt_einvoice_res_code;
        
        if (record.fpt_einvoice_res_msg == null) {
            item.message = "Tạo mới thành công";
            item.status = "success";
        } else {
            item.message = record.fpt_einvoice_res_msg;
            item.status = "error";
        }
        
        item.res_resource = "fpt";
        item.code = null;
        
        if (record.fpt_einvoice_res_json != null) {
            item.data = objectMapper.readTree(record.fpt_einvoice_res_json);
        }
        
        return item;
    }

    private InvoiceResponsePacket.InvoiceResponseItem createInvOutResponseItem(AsyncInvOutRecord record) throws Exception {
        InvoiceResponsePacket.InvoiceResponseItem item = new InvoiceResponsePacket.InvoiceResponseItem();
        item.sid = record.getSid();
        item.sync_sid = record.getSyncid();
        item.message = null;
        item.status = null;
        item.code = null;
        item.res_code = null;
        item.res_resource = "gdt";
        
        if (record.gdt_res != null) {
            item.data = objectMapper.readTree(record.gdt_res);
        }
        
        return item;
    }
} 