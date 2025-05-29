package com.thaihoc.process.response;

import com.fasterxml.jackson.databind.ObjectMapper;
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

public class InvoiceResponseBatchProcessor extends KeyedProcessFunction<Byte, RecordInterface, String> {
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
    
    // Define output tag for database operations (transactional)
    public static final OutputTag<List<RecordInterface>> DATABASE_OPERATIONS_TAG = new OutputTag<>("database-operations") {};

    public InvoiceResponseBatchProcessor(int batchSize, long batchTimeoutMs) {
        this.batchSize = batchSize;
        this.batchTimeoutMs = batchTimeoutMs;
        this.maxWaitTimeMs = batchTimeoutMs * 2;
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
    public void processElement(RecordInterface record, Context ctx, Collector<String> out) throws Exception {
        // Check if record was already processed to avoid duplicates
        Set<String> processedRecords = processedRecordsState.value();
        if (processedRecords == null) {
            processedRecords = new HashSet<>();
        }
        
        String recordKey = keyGenerator.generateRecordKey(record);
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

        // Check if batch is full
        if (currentBuffer.size() >= batchSize) {
            processBatch(currentBuffer, ctx);
            recordsBuffer.clear();
            
            // Mark records as processed
            for (RecordInterface bufferedRecord : currentBuffer) {
                processedRecords.add(keyGenerator.generateRecordKey(bufferedRecord));
            }
            processedRecordsState.update(processedRecords);

            // Clear any existing timer
            timerManager.clearActiveTimer(ctx, activeTimerTimestampState);
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
        
        // Step 1: Send to Kafka first
        kafkaRouter.routeKafkaBatch(records, ctx);
        
        // Step 2: Send records for transactional database operations
        ctx.output(DATABASE_OPERATIONS_TAG, new ArrayList<>(records));
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
        
        // Register new timer
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
} 