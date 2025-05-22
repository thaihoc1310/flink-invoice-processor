package com.thaihoc.process;

import com.thaihoc.model.InvoiceMysqlRecord;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.configuration.Configuration;
import java.util.List;

public class InvoiceProcessingRouter extends ProcessFunction<String, InvoiceMysqlRecord> {
    private final int currentRetryAttempt;
    private final int maxGroupIdValue;

    private final OutputTag<String> retry1Output;
    private final OutputTag<String> retry2Output;
    private final OutputTag<String> retry3Output;
    private final OutputTag<String> dlqOutput;

    private transient InvoiceTransformer transformer;

    public InvoiceProcessingRouter(int currentRetryAttempt, int maxGroupIdValue,
                                   OutputTag<String> retry1Output, OutputTag<String> retry2Output,
                                   OutputTag<String> retry3Output, OutputTag<String> dlqOutput) {
        this.currentRetryAttempt = currentRetryAttempt;
        this.maxGroupIdValue = maxGroupIdValue;
        this.retry1Output = retry1Output;
        this.retry2Output = retry2Output;
        this.retry3Output = retry3Output;
        this.dlqOutput = dlqOutput;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        transformer = new InvoiceTransformer(maxGroupIdValue);
    }

    @Override
    public void processElement(String jsonString, Context ctx, Collector<InvoiceMysqlRecord> out) {
        try {
            List<InvoiceMysqlRecord> records = transformer.transform(jsonString);
            for (InvoiceMysqlRecord record : records) {
                record.retry = (byte) this.currentRetryAttempt;
                out.collect(record);
            }
        } catch (Exception e) {
            System.out.println("Failed to process message (attempt " + currentRetryAttempt + "): "
                    + e.getMessage() + ". Routing for retry/DLQ. Message: " + jsonString);
            if (currentRetryAttempt == 0 && retry1Output != null) {
                ctx.output(retry1Output, jsonString);
            } else if (currentRetryAttempt == 1 && retry2Output != null) {
                ctx.output(retry2Output, jsonString);
            } else if (currentRetryAttempt == 2 && retry3Output != null) {
                ctx.output(retry3Output, jsonString);
            } else {
                ctx.output(dlqOutput, jsonString);
            }
        }
    }
}