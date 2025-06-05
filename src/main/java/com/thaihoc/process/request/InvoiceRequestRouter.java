package com.thaihoc.process.request;

import com.thaihoc.model.request.InvoiceMysqlRecord;
import com.thaihoc.model.retry.InvoiceRetryRecord;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import java.util.List;

public class InvoiceRequestRouter extends ProcessFunction<Object, InvoiceMysqlRecord> {
    private final int maxGroupIdValue;
    private final int maxRetriesConfig;
    private final long retryIntervalMs;
    private transient InvoiceRequestTransformer transformer;

    public InvoiceRequestRouter(int maxGroupIdValue, int maxRetriesConfig, long retryIntervalMs) {
        this.maxGroupIdValue = maxGroupIdValue;
        this.maxRetriesConfig = maxRetriesConfig;
        this.retryIntervalMs = retryIntervalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        transformer = new InvoiceRequestTransformer(maxGroupIdValue, retryIntervalMs);
    }

    @Override
    public void processElement(Object payload, Context ctx, Collector<InvoiceMysqlRecord> out) {
        try {
            if (payload instanceof String) {
                List<InvoiceMysqlRecord> records = this.transformer.transform((String) payload,ctx);
                System.out.println("record_size" + records.size());
                for (InvoiceMysqlRecord record : records) {
                    out.collect(record);
                }
            } else if (payload instanceof InvoiceRetryRecord) {
                InvoiceMysqlRecord record = this.transformer.transformRetryRecord((InvoiceRetryRecord) payload,ctx,maxRetriesConfig);
                if (record != null) {
                    out.collect(record);
                }
            }
        } catch (Exception e) {
            System.err.println("Unexpected error in InvoiceRequestRouterWithRetry: " + e.getMessage());
        }
    }
}