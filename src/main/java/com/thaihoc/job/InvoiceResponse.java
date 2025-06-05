package com.thaihoc.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thaihoc.config.ConfigKeys;
import com.thaihoc.model.response.AsyncInvInRecord;
import com.thaihoc.model.response.AsyncInvOutRecord;
import com.thaihoc.model.response.RecordInterface;
import com.thaihoc.model.retry.InvoiceRetryRecord;
import com.thaihoc.process.response.InvoiceResponseBatchProcessor;
import com.thaihoc.process.response.InvoiceResponseKafkaRouter;
import com.thaihoc.sink.InvoiceRetrySink;
import com.thaihoc.sink.TransactionalLogAndDeleteSink;
import com.thaihoc.source.AsyncInvInSource;
import com.thaihoc.source.AsyncInvOutSource;
import com.thaihoc.source.InvoiceRetrySource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.thaihoc.util.FlinkJobUtils.createKafkaSink;
import static com.thaihoc.util.FlinkJobUtils.loadParameters;

public class InvoiceResponse {

        public static void main(String[] args) throws Exception {
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

                ParameterTool params = loadParameters(args);

                // Load parallelism configurations
                final int defaultJobParallelism = params.getInt(ConfigKeys.FLINK_JOB_PARALLELISM, 1);
                final int mysqlSourceParallelism = params.getInt(ConfigKeys.RESPONSE_MYSQL_SOURCE_PARALLELISM, defaultJobParallelism);
                final int batchProcessorParallelism = params.getInt(ConfigKeys.RESPONSE_BATCH_PROCESSOR_PARALLELISM, defaultJobParallelism);
                final int kafkaSinkParallelism = params.getInt(ConfigKeys.RESPONSE_KAFKA_SINK_PARALLELISM, defaultJobParallelism);
                final int transactionalSinkParallelism = params.getInt(ConfigKeys.RESPONSE_TRANSACTIONAL_SINK_PARALLELISM, defaultJobParallelism);
                
                env.setParallelism(defaultJobParallelism);

                // Database configuration
                final String jdbcUrl = params.getRequired(ConfigKeys.MYSQL_JDBC_URL);
                final String dbUsername = params.getRequired(ConfigKeys.MYSQL_USERNAME);
                final String dbPassword = params.getRequired(ConfigKeys.MYSQL_PASSWORD);
                final long pollingIntervalMs = params.getLong(ConfigKeys.MYSQL_POLLING_INTERVAL_MS, 5000);
                final int fetchSize = params.getInt(ConfigKeys.MYSQL_FETCH_SIZE, 1000);
                final int sqlMaxRetries = params.getInt(ConfigKeys.MYSQL_MAX_RETRIES, 3);

                // Retry configuration
                final int maxRetries = params.getInt(ConfigKeys.APP_MAX_RETRIES, 3);
                final long retryIntervalMs = params.getLong(ConfigKeys.APP_RETRY_INTERVAL_MS, 5000);
                final long retryPollingIntervalMs = params.getLong(ConfigKeys.RETRY_MYSQL_POLLING_INTERVAL_MS, 10000);
                final int retryFetchSize = params.getInt(ConfigKeys.RETRY_MYSQL_FETCH_SIZE, 100);
                final int retrySourceParallelism = params.getInt(ConfigKeys.RETRY_MYSQL_SOURCE_PARALLELISM, defaultJobParallelism);
                final int retrySinkParallelism = params.getInt(ConfigKeys.RETRY_MYSQL_SINK_PARALLELISM, defaultJobParallelism);
                // Create data streams with explicit parallelism
                DataStream<AsyncInvInRecord> invInStream = env.addSource(
                                new AsyncInvInSource(jdbcUrl, dbUsername, dbPassword, pollingIntervalMs, fetchSize))
                                .name("MySQL Source (async_inv_in)")
                                .setParallelism(mysqlSourceParallelism);

                DataStream<AsyncInvOutRecord> invOutStream = env.addSource(
                                new AsyncInvOutSource(jdbcUrl, dbUsername, dbPassword, pollingIntervalMs, fetchSize))
                                .name("MySQL Source (async_inv_out)")
                                .setParallelism(mysqlSourceParallelism);

                // Create retry source for response job
                DataStream<InvoiceRetryRecord> retryStream = env.addSource(
                                new InvoiceRetrySource(jdbcUrl, dbUsername, dbPassword, "RESPONSE", retryPollingIntervalMs, retryFetchSize))
                                .name("Response Retry Source")
                                .setParallelism(retrySourceParallelism);

                // Create Kafka sinks
                KafkaSink<String> crtResponseSink = createKafkaSink(params, ConfigKeys.KAFKA_TOPIC_CRT_RESPONSE);
                KafkaSink<String> updResponseSink = createKafkaSink(params, ConfigKeys.KAFKA_TOPIC_UPD_RESPONSE);
                KafkaSink<String> delResponseSink = createKafkaSink(params, ConfigKeys.KAFKA_TOPIC_DEL_RESPONSE);
                KafkaSink<String> repResponseSink = createKafkaSink(params, ConfigKeys.KAFKA_TOPIC_REP_RESPONSE);
                KafkaSink<String> adjResponseSink = createKafkaSink(params, ConfigKeys.KAFKA_TOPIC_ADJ_RESPONSE);

                // Response batch configuration
                final int responseBatchSize = params.getInt(ConfigKeys.RESPONSE_BATCH_SIZE, 100);
                final long responseBatchTimeoutMs = params.getLong(ConfigKeys.RESPONSE_BATCH_TIMEOUT_MS, 3000);

                // Combine streams for unified processing - convert all to Object type
                DataStream<RecordInterface> InvInRecords = invInStream.map(record -> (RecordInterface) record);
                DataStream<RecordInterface> InvOutRecords = invOutStream.map(record -> (RecordInterface) record);
                DataStream<RecordInterface> allRecords = InvInRecords.union(InvOutRecords);

                DataStream<Object> allStreams = allRecords.map(record -> (Object) record)
                                .union(retryStream.map(record -> (Object) record));

                InvoiceResponseBatchProcessor batchProcessor = new InvoiceResponseBatchProcessor(responseBatchSize,
                                responseBatchTimeoutMs, retryIntervalMs, maxRetries);
                
                SingleOutputStreamOperator<String> processedBatches = allStreams
                                .keyBy(new KeySelector<Object, Byte>() {
                                        @Override
                                        public Byte getKey(Object element) throws Exception {
                                                if (element instanceof RecordInterface) {
                                                        return ((RecordInterface) element).getApiType();
                                                } else if (element instanceof InvoiceRetryRecord) {
                                                        try {
                                                                InvoiceRetryRecord retryRecord = (InvoiceRetryRecord) element;
                                                                ObjectMapper mapper = new ObjectMapper();
                                                                JsonNode node = mapper.readTree(retryRecord.payload);
                                                                if (node.has("api_type")) {
                                                                        return (byte) node.get("api_type").asInt();
                                                                }
                                                        } catch (Exception ignored) {
                                                        }
                                                        return (byte) 0; 
                                                } else {
                                                        return (byte) 0; 
                                                }
                                        }
                                })
                                .process(batchProcessor)
                                .name("Invoice Response Batch Processing")
                                .setParallelism(batchProcessorParallelism);

                // Kafka sinks with explicit parallelism
                processedBatches.getSideOutput(InvoiceResponseKafkaRouter.CRT_OUTPUT_TAG)
                                .sinkTo(crtResponseSink)
                                .name("Sink CRT Batch Responses")
                                .setParallelism(kafkaSinkParallelism);
                processedBatches.getSideOutput(InvoiceResponseKafkaRouter.UPD_OUTPUT_TAG)
                                .sinkTo(updResponseSink)
                                .name("Sink UPD Batch Responses")
                                .setParallelism(kafkaSinkParallelism);
                processedBatches.getSideOutput(InvoiceResponseKafkaRouter.DEL_OUTPUT_TAG)
                                .sinkTo(delResponseSink)
                                .name("Sink DEL Batch Responses")
                                .setParallelism(kafkaSinkParallelism);
                processedBatches.getSideOutput(InvoiceResponseKafkaRouter.REP_OUTPUT_TAG)
                                .sinkTo(repResponseSink)
                                .name("Sink REP Batch Responses")
                                .setParallelism(kafkaSinkParallelism);
                processedBatches.getSideOutput(InvoiceResponseKafkaRouter.ADJ_OUTPUT_TAG)
                                .sinkTo(adjResponseSink)
                                .name("Sink ADJ Batch Responses")
                                .setParallelism(kafkaSinkParallelism);

                // Database operations sink
                TransactionalLogAndDeleteSink transactionalProcessor = new TransactionalLogAndDeleteSink(
                                jdbcUrl, dbUsername, dbPassword, sqlMaxRetries);

                processedBatches.getSideOutput(InvoiceResponseBatchProcessor.DATABASE_OPERATIONS_TAG)
                                .addSink(transactionalProcessor)
                                .name("Transactional Database Operations")
                                .setParallelism(transactionalSinkParallelism);

                // Retry handling - collect all retry operations and send to retry sink
                DataStream<InvoiceRetryRecord> allRetryRecords = processedBatches.getSideOutput(InvoiceResponseBatchProcessor.CREATE_RETRY_TAG)
                                .union(processedBatches.getSideOutput(InvoiceResponseBatchProcessor.UPDATE_RETRY_TAG))
                                .union(processedBatches.getSideOutput(InvoiceResponseBatchProcessor.DELETE_RETRY_TAG))
                                .union(processedBatches.getSideOutput(InvoiceResponseBatchProcessor.MAX_RETRY_TAG));

                InvoiceRetrySink retrySink = new InvoiceRetrySink(jdbcUrl, dbUsername, dbPassword, sqlMaxRetries);
                allRetryRecords.addSink(retrySink)
                                .name("Response Retry Sink")
                                .setParallelism(retrySinkParallelism);

                env.execute("Invoice Response Job");
        }
}
