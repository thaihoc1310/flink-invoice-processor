package com.thaihoc.job;

import com.thaihoc.config.ConfigKeys;
import com.thaihoc.model.AsyncInvInRecord;
import com.thaihoc.model.AsyncInvOutRecord;
import com.thaihoc.model.RecordInterface;
import com.thaihoc.process.response.SequentialInvoiceProcessor;
import com.thaihoc.sink.TransactionalLogAndDeleteSink;
import com.thaihoc.source.AsyncInvInSource;
import com.thaihoc.source.AsyncInvOutSource;
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

                final int defaultJobParallelism = params.getInt(ConfigKeys.FLINK_JOB_PARALLELISM, 1);
                final int mySqlSinkParallelism = params.getInt(ConfigKeys.MYSQL_SINK_PARALLELISM, 1);
                env.setParallelism(defaultJobParallelism);

                // Database configuration
                final String jdbcUrl = params.getRequired(ConfigKeys.MYSQL_JDBC_URL);
                final String dbUsername = params.getRequired(ConfigKeys.MYSQL_USERNAME);
                final String dbPassword = params.getRequired(ConfigKeys.MYSQL_PASSWORD);
                final long pollingIntervalMs = params.getLong(ConfigKeys.MYSQL_POLLING_INTERVAL_MS, 5000);
                final int fetchSize = params.getInt(ConfigKeys.MYSQL_FETCH_SIZE, 1000);
                final int sqlMaxRetries = params.getInt(ConfigKeys.MYSQL_MAX_RETRIES, 3);

                // Create data streams
                DataStream<AsyncInvInRecord> invInStream = env.addSource(
                                new AsyncInvInSource(jdbcUrl, dbUsername, dbPassword, pollingIntervalMs, fetchSize))
                                .name("MySQL Source (async_inv_in)");

                DataStream<AsyncInvOutRecord> invOutStream = env.addSource(
                                new AsyncInvOutSource(jdbcUrl, dbUsername, dbPassword, pollingIntervalMs, fetchSize))
                                .name("MySQL Source (async_inv_out)");

                // Create Kafka sinks
                KafkaSink<String> crtResponseSink = createKafkaSink(params, ConfigKeys.KAFKA_TOPIC_CRT_RESPONSE);
                KafkaSink<String> updResponseSink = createKafkaSink(params, ConfigKeys.KAFKA_TOPIC_UPD_RESPONSE);
                KafkaSink<String> delResponseSink = createKafkaSink(params, ConfigKeys.KAFKA_TOPIC_DEL_RESPONSE);
                KafkaSink<String> repResponseSink = createKafkaSink(params, ConfigKeys.KAFKA_TOPIC_REP_RESPONSE);
                KafkaSink<String> adjResponseSink = createKafkaSink(params, ConfigKeys.KAFKA_TOPIC_ADJ_RESPONSE);

                // Response batch configuration
                final int responseBatchSize = params.getInt(ConfigKeys.RESPONSE_BATCH_SIZE, 100);
                final long responseBatchTimeoutMs = params.getLong(ConfigKeys.RESPONSE_BATCH_TIMEOUT_MS, 5000);

                // Combine both streams for unified processing
                DataStream<RecordInterface> InvInRecords = invInStream.map(record -> (RecordInterface) record);
                DataStream<RecordInterface> InvOutRecords = invOutStream.map(record -> (RecordInterface) record);
                DataStream<RecordInterface> allRecords = InvInRecords.union(InvOutRecords);

                SequentialInvoiceProcessor sequentialProcessor = new SequentialInvoiceProcessor(responseBatchSize,
                                responseBatchTimeoutMs);
                SingleOutputStreamOperator<String> processedBatches = allRecords
                                .keyBy(new KeySelector<RecordInterface, Byte>() {
                                        @Override
                                        public Byte getKey(RecordInterface record) throws Exception {
                                                return record.getApiType();
                                        }
                                })
                                .process(sequentialProcessor)
                                .name("Sequential Invoice Processing");

                processedBatches.getSideOutput(SequentialInvoiceProcessor.CRT_OUTPUT_TAG)
                                .sinkTo(crtResponseSink).name("Sink CRT Batch Responses");
                processedBatches.getSideOutput(SequentialInvoiceProcessor.UPD_OUTPUT_TAG)
                                .sinkTo(updResponseSink).name("Sink UPD Batch Responses");
                processedBatches.getSideOutput(SequentialInvoiceProcessor.DEL_OUTPUT_TAG)
                                .sinkTo(delResponseSink).name("Sink DEL Batch Responses");
                processedBatches.getSideOutput(SequentialInvoiceProcessor.REP_OUTPUT_TAG)
                                .sinkTo(repResponseSink).name("Sink REP Batch Responses");
                processedBatches.getSideOutput(SequentialInvoiceProcessor.ADJ_OUTPUT_TAG)
                                .sinkTo(adjResponseSink).name("Sink ADJ Batch Responses");

                TransactionalLogAndDeleteSink transactionalProcessor = new TransactionalLogAndDeleteSink(
                                jdbcUrl, dbUsername, dbPassword, sqlMaxRetries);

                processedBatches.getSideOutput(SequentialInvoiceProcessor.DATABASE_OPERATIONS_TAG)
                                .addSink(transactionalProcessor)
                                .name("Transactional Database Operations")
                                .setParallelism(mySqlSinkParallelism);

                env.execute("Invoice Response Job");
        }
}
