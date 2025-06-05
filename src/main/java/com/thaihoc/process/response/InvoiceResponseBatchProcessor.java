package com.thaihoc.process.response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thaihoc.model.response.AsyncInvInRecord;
import com.thaihoc.model.response.AsyncInvOutRecord;
import com.thaihoc.model.response.RecordInterface;
import com.thaihoc.model.retry.InvoiceRetryRecord;
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

public class InvoiceResponseBatchProcessor extends KeyedProcessFunction<Byte, Object, String> {
    private transient ObjectMapper objectMapper;
    private transient ListState<RecordInterface> recordsBuffer;
    private transient ValueState<Set<String>> processedRecordsState;
    private transient ValueState<Long> activeTimerTimestampState;
    private transient ValueState<Long> lastProcessedTimeState;
    
    private transient InvoiceResponseItemFactory itemFactory;
    private transient InvoiceResponseKafkaRouter kafkaRouter;
    private transient InvoiceResponseTimerManager timerManager;
    private transient InvoiceResponseRecordKeyGenerator keyGenerator;
    
    private final int batchSize;
    private final long batchTimeoutMs;
    private final long maxWaitTimeMs;
    private final long retryIntervalMs;
    private final int maxRetriesConfig;
    
    // Define output tag for database operations (transactional)
    public static final OutputTag<List<RecordInterface>> DATABASE_OPERATIONS_TAG = new OutputTag<>("database-operations") {};
    
    // Define retry output tags
    public static final OutputTag<InvoiceRetryRecord> CREATE_RETRY_TAG = new OutputTag<>("create-retry") {};
    public static final OutputTag<InvoiceRetryRecord> UPDATE_RETRY_TAG = new OutputTag<>("update-retry") {};
    public static final OutputTag<InvoiceRetryRecord> DELETE_RETRY_TAG = new OutputTag<>("delete-retry") {};
    public static final OutputTag<InvoiceRetryRecord> MAX_RETRY_TAG = new OutputTag<>("max-retry") {};

    public InvoiceResponseBatchProcessor(int batchSize, long batchTimeoutMs, long retryIntervalMs, int maxRetriesConfig) {
        this.batchSize = batchSize;
        this.batchTimeoutMs = batchTimeoutMs;
        this.maxWaitTimeMs = batchTimeoutMs * 2;
        this.retryIntervalMs = retryIntervalMs;
        this.maxRetriesConfig = maxRetriesConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        objectMapper = new ObjectMapper();
        
        // Initialize components
        itemFactory = new InvoiceResponseItemFactory(objectMapper);
        kafkaRouter = new InvoiceResponseKafkaRouter(objectMapper, itemFactory);
        timerManager = new InvoiceResponseTimerManager(batchTimeoutMs, maxWaitTimeMs);
        keyGenerator = new InvoiceResponseRecordKeyGenerator();
        
        ListStateDescriptor<RecordInterface> bufferDescriptor = new ListStateDescriptor<>(
                "recordsBuffer",
                TypeInformation.of(new TypeHint<>() {
                })
        );
        recordsBuffer = getRuntimeContext().getListState(bufferDescriptor);
        
        ValueStateDescriptor<Set<String>> processedDescriptor = new ValueStateDescriptor<>(
                "processedRecords",
                TypeInformation.of(new TypeHint<>() {
                })
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
    public void processElement(Object element, Context ctx, Collector<String> out) throws Exception {
        if (element instanceof RecordInterface) {
            processRecordInterface((RecordInterface) element, ctx, out);
        } else if (element instanceof InvoiceRetryRecord) {
            processRetryRecord((InvoiceRetryRecord) element, ctx, out);
        } else {
            System.err.println("Unknown element type: " + element.getClass().getSimpleName());
        }
    }
    
    private void processRecordInterface(RecordInterface record, Context ctx, Collector<String> out) throws Exception {
        // Check if record was already processed to avoid duplicates
        Set<String> processedRecords = processedRecordsState.value();
        if (processedRecords == null) {
            processedRecords = new HashSet<>();
        }
        
        String recordKey = keyGenerator.generateRecordKey(record);
        if (processedRecords.contains(recordKey)) {
            return;
        }

        recordsBuffer.add(record);  

        List<RecordInterface> currentBuffer = new ArrayList<>();
        for (RecordInterface bufferedRecord : recordsBuffer.get()) {
            currentBuffer.add(bufferedRecord);
        }

        // Check if batch is full
        if (currentBuffer.size() >= batchSize) {
            processBatch(currentBuffer, ctx);
            recordsBuffer.clear();
            
            // Mark records as processed
            for (RecordInterface bufferedRecord : currentBuffer) {
                processedRecords.add(keyGenerator.generateRecordKey(bufferedRecord));
            }
            processedRecordsState.update(processedRecords);

            timerManager.clearActiveTimer(ctx, activeTimerTimestampState);
        } else {
            manageTimer(ctx, currentBuffer);
        }
    }
    
    private void processRetryRecord(InvoiceRetryRecord retryRecord, Context ctx, Collector<String> out) throws Exception {
        try {
            RecordInterface record = processRetryRecordInternal(retryRecord, ctx);
            if (record != null) {
                // Process the recovered record normally
                processRecordInterface(record, ctx, out);
            }
        } catch (Exception e) {
            System.err.println("Error processing retry record: " + e.getMessage());
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
                processedRecords.add(keyGenerator.generateRecordKey(bufferedRecord));
            }
            processedRecordsState.update(processedRecords);
        }
        
        // Clear the timer state since it has been processed
        timerManager.clearActiveTimer(ctx, activeTimerTimestampState);
    }

    private void processBatch(List<RecordInterface> records, Context ctx) throws Exception {
        if (records.isEmpty()) return;
        
        // Update last processed time
        long currentTime = System.currentTimeMillis();
        lastProcessedTimeState.update(currentTime);

        List<RecordInterface> successfulRecords = new ArrayList<>();
        
        for (RecordInterface record : records) {
            try {
                itemFactory.createResponseItem(record);
                successfulRecords.add(record);
            } catch (Exception e) {
                System.err.println("Failed to process record for sid: " + record.getSid() + ", error: " + e.getMessage());
                addIntoCreateRetryOutput(record, e, "CREATE", retryIntervalMs / 1000, ctx);
            }
        }
        
        // Only process successful records
        if (!successfulRecords.isEmpty()) {
            try {
                // Step 1: Send to Kafka first
                kafkaRouter.routeKafkaBatch(successfulRecords, ctx);
                
                // Step 2: Send records for transactional database operations
                ctx.output(DATABASE_OPERATIONS_TAG, new ArrayList<>(successfulRecords));
            } catch (Exception e) {
                System.err.println("Failed to route batch to Kafka/Database: " + e.getMessage());
                
                for (RecordInterface record : successfulRecords) {
                    addIntoCreateRetryOutput(record, e, "CREATE", retryIntervalMs / 1000, ctx);
                }
            }
        }
    }

    private void addIntoCreateRetryOutput(RecordInterface record, Exception e, String tag, long nextRetryTime, Context ctx) throws Exception {
        InvoiceRetryRecord retryRecord = createRetryRecord(record, e);
        retryRecord.tag = tag;
        retryRecord.next_retry_time = nextRetryTime;
        ctx.output(CREATE_RETRY_TAG, retryRecord);
    }

    private void manageTimer(Context ctx, List<RecordInterface> currentBuffer) throws Exception {
        // Check if we should force process due to max wait time
        if (timerManager.shouldForceProcessBatch(lastProcessedTimeState) && !currentBuffer.isEmpty()) {
            processBatch(currentBuffer, ctx);
            recordsBuffer.clear();
            
            // Mark records as processed
            Set<String> processedRecords = processedRecordsState.value();
            if (processedRecords == null) {
                processedRecords = new HashSet<>();
            }
            for (RecordInterface bufferedRecord : currentBuffer) {
                processedRecords.add(keyGenerator.generateRecordKey(bufferedRecord));
            }
            processedRecordsState.update(processedRecords);
            
            // Clear timer
            timerManager.clearActiveTimer(ctx, activeTimerTimestampState);
            return;
        }
        
        try {
            timerManager.registerTimer(ctx, activeTimerTimestampState);
        } catch (Exception e) {
            System.err.println("Error setting timer: " + e.getMessage());
            if (!currentBuffer.isEmpty()) {
                processBatch(currentBuffer, ctx);
                recordsBuffer.clear();
            }
        }
    }
    

    private InvoiceRetryRecord createRetryRecord(RecordInterface record, Exception exception) throws JsonProcessingException {
        InvoiceRetryRecord retryRecord = new InvoiceRetryRecord();
        retryRecord.payload = objectMapper.writeValueAsString(record);
        retryRecord.state = "PENDING";
        retryRecord.job = "RESPONSE";
        retryRecord.error_message = exception.getMessage();
        retryRecord.sid = record.getSid();
        retryRecord.syncid = record.getSyncid();
        retryRecord.retry_count = 0;
        retryRecord.error_code = exception.getClass().getSimpleName();
        return retryRecord;
    }
    

    private RecordInterface processRetryRecordInternal(InvoiceRetryRecord retryRecord, Context ctx) throws Exception {
        if (retryRecord.retry_count > maxRetriesConfig) {
            retryRecord.tag = "MAX_RETRY";
            ctx.output(MAX_RETRY_TAG, retryRecord);
            return null;
        }
        
        try {
            RecordInterface record = deserializeRetryPayload(retryRecord.payload);
            if(record.getApiType() != 10 && record.getApiType() != 11 && record.getApiType() != 12 && record.getApiType() != 13 && record.getApiType() != 14) {
                throw new Exception("Unknown api_type: " + record.getApiType());
            }
            itemFactory.createResponseItem(record);
            retryRecord.tag = "DELETE";
            ctx.output(DELETE_RETRY_TAG, retryRecord);
            
            return record;
        } catch (Exception e) {
            retryRecord.retry_count++;
            retryRecord.error_message = e.getMessage();
            retryRecord.error_code = e.getClass().getSimpleName();
            retryRecord.tag = "UPDATE";
            retryRecord.next_retry_time = (retryIntervalMs / 1000) * (long) Math.pow(2, retryRecord.retry_count);
            ctx.output(UPDATE_RETRY_TAG, retryRecord);
            
            return null;
        }
    }
    

    private RecordInterface deserializeRetryPayload(String payload) throws Exception {
        JsonNode node = objectMapper.readTree(payload);
        
        if (node.has("fpt_einvoice_res_code") || node.has("fpt_einvoice_res_msg") || node.has("fpt_einvoice_res_json")) {
            return objectMapper.readValue(payload, AsyncInvInRecord.class);
        } else if (node.has("gdt_res")) {
            return objectMapper.readValue(payload, AsyncInvOutRecord.class);
        } else {
            throw new Exception("Unknown record type");
        }
    }
} 