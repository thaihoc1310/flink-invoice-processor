package com.thaihoc.process.response;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thaihoc.model.AsyncInvInRecord;
import com.thaihoc.model.AsyncInvOutRecord;
import com.thaihoc.model.InvoiceResponsePacket;
import com.thaihoc.model.RecordInterface;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SequentialInvoiceProcessor extends KeyedProcessFunction<Byte, RecordInterface, String> {
    private transient ObjectMapper objectMapper;
    private transient ListState<RecordInterface> recordsBuffer;
    private transient ValueState<Set<String>> processedRecordsState;
    private transient ValueState<Long> activeTimerTimestampState;
    private transient ValueState<Long> lastProcessedTimeState; // Track last processing time
    private final int batchSize;
    private final long batchTimeoutMs;
    private final long maxWaitTimeMs;

    // Define output tags for different API types (Kafka outputs)
    public static final OutputTag<String> CRT_OUTPUT_TAG = new OutputTag<String>("crt-response-batch") {};
    public static final OutputTag<String> UPD_OUTPUT_TAG = new OutputTag<String>("upd-response-batch") {};
    public static final OutputTag<String> DEL_OUTPUT_TAG = new OutputTag<String>("del-response-batch") {};
    public static final OutputTag<String> REP_OUTPUT_TAG = new OutputTag<String>("rep-response-batch") {};
    public static final OutputTag<String> ADJ_OUTPUT_TAG = new OutputTag<String>("adj-response-batch") {};
    
    // Define output tag for database operations (transactional)
    public static final OutputTag<List<RecordInterface>> DATABASE_OPERATIONS_TAG = new OutputTag<>("database-operations") {};

    private static final int EMERGENCY_FLUSH_SIZE = 4000;
    public SequentialInvoiceProcessor(int batchSize, long batchTimeoutMs) {
        this.batchSize = batchSize;
        this.batchTimeoutMs = batchTimeoutMs;
        this.maxWaitTimeMs = batchTimeoutMs * 3;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        objectMapper = new ObjectMapper();
        
        ListStateDescriptor<RecordInterface> bufferDescriptor = new ListStateDescriptor<>(
                "recordsBuffer",
                TypeInformation.of(new TypeHint<RecordInterface>() {})
        );
        recordsBuffer = getRuntimeContext().getListState(bufferDescriptor);
        
        ValueStateDescriptor<Set<String>> processedDescriptor = new ValueStateDescriptor<>(
                "processedRecords",
                TypeInformation.of(new TypeHint<Set<String>>() {})
        );
        processedRecordsState = getRuntimeContext().getState(processedDescriptor);

        ValueStateDescriptor<Long> activeTimerDescriptor = new ValueStateDescriptor<>(
                "activeTimerTimestamp",
                TypeInformation.of(Long.class)
        );
        activeTimerTimestampState = getRuntimeContext().getState(activeTimerDescriptor);

        ValueStateDescriptor<Long> lastProcessedTimeDescriptor = new ValueStateDescriptor<>(
                "lastProcessedTime",
                TypeInformation.of(Long.class)
        );
        lastProcessedTimeState = getRuntimeContext().getState(lastProcessedTimeDescriptor);
    }

    @Override
    public void processElement(RecordInterface record, Context ctx, Collector<String> out) throws Exception {
        // Check if record was already processed to avoid duplicates
        Set<String> processedRecords = processedRecordsState.value();
        if (processedRecords == null) {
            processedRecords = new HashSet<>();
        }
        
        String recordKey = generateRecordKey(record);
        if (processedRecords.contains(recordKey)) {
            return;
        }
        
        // Add record to buffer
        recordsBuffer.add(record);  
        
        // Get current buffer size
        List<RecordInterface> currentBuffer = new ArrayList<>();
        for (RecordInterface bufferedRecord : recordsBuffer.get()) {
            currentBuffer.add(bufferedRecord);
        }

        
        // Check for emergency flush first
        if (shouldEmergencyFlush(currentBuffer)) {
            emergencyFlush(currentBuffer, ctx);
            recordsBuffer.clear();
            
            // Mark all records as processed
            for (RecordInterface bufferedRecord : currentBuffer) {
                processedRecords.add(generateRecordKey(bufferedRecord));
            }
            processedRecordsState.update(processedRecords);
            clearActiveTimer(ctx);
            return;
        }
        
        // Check if batch is full
            if (currentBuffer.size() >= batchSize) {
            processBatch(currentBuffer, ctx);
            recordsBuffer.clear();
            
            // Mark records as processed
            for (RecordInterface bufferedRecord : currentBuffer) {
                processedRecords.add(generateRecordKey(bufferedRecord));
            }
            processedRecordsState.update(processedRecords);

            // Clear any existing timer
            clearActiveTimer(ctx);
        } else {
            // Use enhanced timer management
            manageTimer(ctx, currentBuffer);
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
            
            // Mark records as processed
            Set<String> processedRecords = processedRecordsState.value();
            if (processedRecords == null) {
                processedRecords = new HashSet<>();
            }
            for (RecordInterface bufferedRecord : currentBuffer) {
                processedRecords.add(generateRecordKey(bufferedRecord));
            }
            processedRecordsState.update(processedRecords);
        }
        
        // Clear the timer state since it has been processed
        clearActiveTimer(ctx);
    }

    private void processBatch(List<RecordInterface> records, Context ctx) throws Exception {
        if (records.isEmpty()) return;
        
        // Update last processed time
        long currentTime = System.currentTimeMillis();
        lastProcessedTimeState.update(currentTime);
        
        // Step 1: Send to Kafka first
        processKafkaBatch(records, ctx);
        
        // Step 2: Send records for transactional database operations
        ctx.output(DATABASE_OPERATIONS_TAG, new ArrayList<>(records));

    }
    

    private boolean shouldForceProcessBatch() throws Exception {
        Long lastProcessedTime = lastProcessedTimeState.value();
        if (lastProcessedTime == null) {
            return false;
        }
        
        long currentTime = System.currentTimeMillis();
        long timeSinceLastProcess = currentTime - lastProcessedTime;
        
        return timeSinceLastProcess >= maxWaitTimeMs;
    }
    

    private void manageTimer(Context ctx, List<RecordInterface> currentBuffer) throws Exception {
        // Check if we should force process due to max wait time
        if (shouldForceProcessBatch() && !currentBuffer.isEmpty()) {
            processBatch(currentBuffer, ctx);
            recordsBuffer.clear();
            
            // Mark records as processed
            Set<String> processedRecords = processedRecordsState.value();
            if (processedRecords == null) {
                processedRecords = new HashSet<>();
            }
            for (RecordInterface bufferedRecord : currentBuffer) {
                processedRecords.add(generateRecordKey(bufferedRecord));
            }
            processedRecordsState.update(processedRecords);
            
            // Clear timer
            clearActiveTimer(ctx);
            return;
        }
        
        // Normal timer management
        Long existingTimer = activeTimerTimestampState.value();
        if (existingTimer != null) {
            try {
                ctx.timerService().deleteProcessingTimeTimer(existingTimer);
                System.out.println("Deleted existing timer before setting new one");
            } catch (Exception e) {
                System.err.println("Error deleting existing timer: " + e.getMessage());
            }
        }
        
        try {
            long currentTime = System.currentTimeMillis();
            long timerTimestamp = currentTime + batchTimeoutMs;
            ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
            activeTimerTimestampState.update(timerTimestamp);

        } catch (Exception e) {
            System.err.println("Error setting timer: " + e.getMessage());
            if (!currentBuffer.isEmpty()) {
                processBatch(currentBuffer, ctx);
                recordsBuffer.clear();
            }
        }
    }
    

    private void clearActiveTimer(Context ctx) throws Exception {
        Long existingTimer = activeTimerTimestampState.value();
        if (existingTimer != null) {
            try {
                ctx.timerService().deleteProcessingTimeTimer(existingTimer);
                activeTimerTimestampState.clear();
                System.out.println("Cleared existing timer");
            } catch (Exception e) {
                System.err.println("Error clearing timer: " + e.getMessage());
                // Clear state anyway
                activeTimerTimestampState.clear();
            }
        }
    }
    
    private void processKafkaBatch(List<RecordInterface> records, Context ctx) throws Exception {
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
    
    private String generateRecordKey(RecordInterface record) {
        if (record instanceof AsyncInvInRecord) {
            AsyncInvInRecord invIn = (AsyncInvInRecord) record;
            return "InvIn_" + invIn.id + "_" + invIn.sid + "_" + invIn.syncid;
        } else if (record instanceof AsyncInvOutRecord) {
            AsyncInvOutRecord invOut = (AsyncInvOutRecord) record;
            return "InvOut_" + invOut.id + "_" + invOut.sid + "_" + invOut.syncid;
        }
        return record.getSid() + "_" + record.getSyncid();
    }

    private boolean shouldEmergencyFlush(List<RecordInterface> currentBuffer) {
        return currentBuffer.size() >= EMERGENCY_FLUSH_SIZE;
    }
    
    private void emergencyFlush(List<RecordInterface> currentBuffer, Context ctx) throws Exception {

        for (int i = 0; i < currentBuffer.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, currentBuffer.size());
            List<RecordInterface> chunk = currentBuffer.subList(i, endIndex);

            processBatch(chunk, ctx);
        }
    }
} 