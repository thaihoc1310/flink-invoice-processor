package com.thaihoc.process.response;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

public class InvoiceResponseTimerManager {
    private final long batchTimeoutMs;
    private final long maxWaitTimeMs;

    public InvoiceResponseTimerManager(long batchTimeoutMs, long maxWaitTimeMs) {
        this.batchTimeoutMs = batchTimeoutMs;
        this.maxWaitTimeMs = maxWaitTimeMs;
    }

    public boolean shouldForceProcessBatch(ValueState<Long> lastProcessedTimeState) throws Exception {
        Long lastProcessedTime = lastProcessedTimeState.value();
        if (lastProcessedTime == null) {
            return false;
        }
        
        long currentTime = System.currentTimeMillis();
        long timeSinceLastProcess = currentTime - lastProcessedTime;
        
        return timeSinceLastProcess >= maxWaitTimeMs;
    }

    public void registerTimer(KeyedProcessFunction<Byte, Object, String>.Context ctx,
                             ValueState<Long> activeTimerTimestampState) throws Exception {
        // Clear existing timer first
        clearActiveTimer(ctx, activeTimerTimestampState);
        
        try {
            long currentTime = System.currentTimeMillis();
            long timerTimestamp = currentTime + batchTimeoutMs;
            ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
            activeTimerTimestampState.update(timerTimestamp);
        } catch (Exception e) {
            System.err.println("Error setting timer: " + e.getMessage());
            throw e;
        }
    }

    public void clearActiveTimer(KeyedProcessFunction<Byte, Object, String>.Context ctx,
                                ValueState<Long> activeTimerTimestampState) throws Exception {
        Long existingTimer = activeTimerTimestampState.value();
        if (existingTimer != null) {
            try {
                ctx.timerService().deleteProcessingTimeTimer(existingTimer);
                activeTimerTimestampState.clear();
//                System.out.println("Cleared existing timer");
            } catch (Exception e) {
                System.err.println("Error clearing timer: " + e.getMessage());
                // Clear state anyway
                activeTimerTimestampState.clear();
            }
        }
    }
} 