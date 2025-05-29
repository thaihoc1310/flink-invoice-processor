package com.thaihoc.job;


import com.thaihoc.config.ConfigKeys;
import com.thaihoc.model.InvoiceMysqlRecord;
import com.thaihoc.process.request.InvoiceRequestRouter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.sql.Types;

import static com.thaihoc.util.FlinkJobUtils.*;

public class InvoiceRequest {
    public static final OutputTag<String> retryOutputTag = new OutputTag<>("retry-output") {
    };
    public static final OutputTag<String> dlqOutputTag = new OutputTag<>("dlq-output") {
    };
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = loadParameters(args);

        // Load parallelism configurations
        final int defaultJobParallelism = params.getInt(ConfigKeys.FLINK_JOB_PARALLELISM, 1);
        final int kafkaSourceParallelism = params.getInt(ConfigKeys.REQUEST_KAFKA_SOURCE_PARALLELISM, defaultJobParallelism);
        final int processorParallelism = params.getInt(ConfigKeys.REQUEST_PROCESSOR_PARALLELISM, defaultJobParallelism);
        final int retrySinkParallelism = params.getInt(ConfigKeys.REQUEST_RETRY_SINK_PARALLELISM, 1);
        final int dlqSinkParallelism = params.getInt(ConfigKeys.REQUEST_DLQ_SINK_PARALLELISM, 1);
        final int mySqlSinkParallelism = params.getInt(ConfigKeys.REQUEST_MYSQL_SINK_PARALLELISM, defaultJobParallelism);
        
        env.setParallelism(defaultJobParallelism);

        KafkaSource<String> crtRequestSource = createKafkaSource(params, ConfigKeys.KAFKA_TOPIC_CRT_REQUEST, ConfigKeys.KAFKA_GROUP_ID_CRT_REQUEST);
        KafkaSource<String> updRequestSource = createKafkaSource(params, ConfigKeys.KAFKA_TOPIC_UPD_REQUEST, ConfigKeys.KAFKA_GROUP_ID_UPD_REQUEST);
        KafkaSource<String> delRequestSource = createKafkaSource(params, ConfigKeys.KAFKA_TOPIC_DEL_REQUEST, ConfigKeys.KAFKA_GROUP_ID_DEL_REQUEST);
        KafkaSource<String> repRequestSource = createKafkaSource(params, ConfigKeys.KAFKA_TOPIC_REP_REQUEST, ConfigKeys.KAFKA_GROUP_ID_REP_REQUEST);
        KafkaSource<String> adjRequestSource = createKafkaSource(params, ConfigKeys.KAFKA_TOPIC_ADJ_REQUEST, ConfigKeys.KAFKA_GROUP_ID_ADJ_REQUEST);

        KafkaSource<String> retrySource = createKafkaSource(params, ConfigKeys.KAFKA_TOPIC_RETRY, ConfigKeys.KAFKA_GROUP_ID_RETRY);



        DataStream<String> crtRequestJsonStream = env.fromSource(crtRequestSource, WatermarkStrategy.noWatermarks(), "Kafka CRT Request Source")
                .setParallelism(kafkaSourceParallelism);
        DataStream<String> updRequestJsonStream = env.fromSource(updRequestSource, WatermarkStrategy.noWatermarks(), "Kafka UPD Request Source")
                .setParallelism(kafkaSourceParallelism);
        DataStream<String> delRequestJsonStream = env.fromSource(delRequestSource, WatermarkStrategy.noWatermarks(), "Kafka DEL Request Source")
                .setParallelism(kafkaSourceParallelism);
        DataStream<String> repRequestJsonStream = env.fromSource(repRequestSource, WatermarkStrategy.noWatermarks(), "Kafka REP Request Source")
                .setParallelism(kafkaSourceParallelism);
        DataStream<String> adjRequestJsonStream = env.fromSource(adjRequestSource, WatermarkStrategy.noWatermarks(), "Kafka ADJ Request Source")
                .setParallelism(kafkaSourceParallelism);

        DataStream<String> retryJsonStream = env.fromSource(retrySource, WatermarkStrategy.noWatermarks(), "Kafka Retry Source")
                .setParallelism(kafkaSourceParallelism);


        KafkaSink<String> retrySink = createKafkaSink(params, ConfigKeys.KAFKA_TOPIC_RETRY);
        KafkaSink<String> dlqSink = createKafkaSink(params, ConfigKeys.KAFKA_TOPIC_DLQ);

        final int maxGroupIdValue = params.getInt(ConfigKeys.APP_GROUP_ID_MAX_VALUE, 4) + 1;
        final int maxRetries = params.getInt(ConfigKeys.APP_MAX_RETRIES, 3);

        InvoiceRequestRouter invoiceProcessor = new InvoiceRequestRouter(maxGroupIdValue, maxRetries, retryOutputTag, dlqOutputTag);

        // Process primary topic stream with explicit parallelism
        SingleOutputStreamOperator<InvoiceMysqlRecord> processedCrtRequest = crtRequestJsonStream
                .process(invoiceProcessor)
                .name("ProcessCrtRequestInvoices")
                .setParallelism(processorParallelism);

        SingleOutputStreamOperator<InvoiceMysqlRecord> processedUpdRequest = updRequestJsonStream
                .process(invoiceProcessor)
                .name("ProcessUpdRequestInvoices")
                .setParallelism(processorParallelism);

        SingleOutputStreamOperator<InvoiceMysqlRecord> processedDelRequest = delRequestJsonStream
                .process(invoiceProcessor)
                .name("ProcessDelRequestInvoices")
                .setParallelism(processorParallelism);

        SingleOutputStreamOperator<InvoiceMysqlRecord> processedRepRequest = repRequestJsonStream
                .process(invoiceProcessor)
                .name("ProcessRepRequestInvoices")
                .setParallelism(processorParallelism);

        SingleOutputStreamOperator<InvoiceMysqlRecord> processedAdjRequest = adjRequestJsonStream
                .process(invoiceProcessor)
                .name("ProcessAdjRequestInvoices")
                .setParallelism(processorParallelism);
        
        // Union all retry side outputs from primary processing and sink them
        DataStream<String> allPrimaryRetryOutputs = processedCrtRequest.getSideOutput(retryOutputTag)
                .union(processedUpdRequest.getSideOutput(retryOutputTag))
                .union(processedDelRequest.getSideOutput(retryOutputTag))
                .union(processedRepRequest.getSideOutput(retryOutputTag))
                .union(processedAdjRequest.getSideOutput(retryOutputTag));
        
        allPrimaryRetryOutputs.sinkTo(retrySink)
                .name("SinkAllPrimaryToRetry")
                .setParallelism(retrySinkParallelism);

        // Process retry topic stream
        SingleOutputStreamOperator<InvoiceMysqlRecord> processedRetry = retryJsonStream
                .process(invoiceProcessor)
                .name("ProcessRetryInvoices")
                .setParallelism(processorParallelism);
                
        processedRetry.getSideOutput(retryOutputTag).sinkTo(retrySink)
                .name("SinkToRetry")
                .setParallelism(retrySinkParallelism);
        processedRetry.getSideOutput(dlqOutputTag).sinkTo(dlqSink)
                .name("SinkToDLQ")
                .setParallelism(dlqSinkParallelism);

        DataStream<String> allPrimaryDlqOutputs = processedCrtRequest.getSideOutput(dlqOutputTag)
                .union(processedUpdRequest.getSideOutput(dlqOutputTag))
                .union(processedDelRequest.getSideOutput(dlqOutputTag))
                .union(processedRepRequest.getSideOutput(dlqOutputTag))
                .union(processedAdjRequest.getSideOutput(dlqOutputTag));

        allPrimaryDlqOutputs.sinkTo(dlqSink)
                .name("SinkAllPrimaryToDLQ")
                .setParallelism(dlqSinkParallelism);

        DataStream<InvoiceMysqlRecord> allSuccessfulRecords = processedCrtRequest
                .union(processedUpdRequest)
                .union(processedDelRequest)
                .union(processedRepRequest)
                .union(processedAdjRequest)
                .union(processedRetry);

        final String jdbcUrl = params.getRequired(ConfigKeys.MYSQL_JDBC_URL);
        final String dbUsername = params.getRequired(ConfigKeys.MYSQL_USERNAME);
        final String dbPassword = params.getRequired(ConfigKeys.MYSQL_PASSWORD);
        final String tableName = params.getRequired(ConfigKeys.MYSQL_TABLE_NAME);

        final int batchSize = params.getInt(ConfigKeys.MYSQL_BATCH_SIZE, 2000);
        final long batchIntervalMs = params.getLong(ConfigKeys.MYSQL_BATCH_INTERVAL_MS, 5000);
        final int sqlMaxRetries = params.getInt(ConfigKeys.MYSQL_MAX_RETRIES, 3);

        String insertSql = "INSERT INTO " + tableName + " (" +
                "tax_schema, inv, api_type, res_type, fpt_einvoice_res_code, " +
                "fpt_einvoice_res_msg, fpt_einvoice_res_json, retry, state, group_id, " +
                "created_date, updated_date, callback_res_code, callback_res_msg, " +
                "callback_res_json, sid, syncid, process_kafka" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        allSuccessfulRecords.addSink(
                JdbcSink.sink(
                        insertSql,
                        (preparedStatement, record) -> {
                            preparedStatement.setString(1, record.tax_schema);
                            preparedStatement.setString(2, record.inv);
                            preparedStatement.setByte(3, record.api_type);
                            preparedStatement.setNull(4, Types.TINYINT);
                            preparedStatement.setString(5, record.fpt_einvoice_res_code);

                            preparedStatement.setString(6, record.fpt_einvoice_res_msg);
                            preparedStatement.setString(7, record.fpt_einvoice_res_json);
                            preparedStatement.setByte(8, record.retry);
                            preparedStatement.setByte(9, record.state);
                            preparedStatement.setByte(10, record.group_id);

                            preparedStatement.setTimestamp(11, record.created_date);
                            preparedStatement.setTimestamp(12, record.updated_date);
                            preparedStatement.setString(13, record.callback_res_code);
                            preparedStatement.setString(14, record.callback_res_msg);

                            preparedStatement.setString(15, record.callback_res_json);
                            preparedStatement.setString(16, record.sid);
                            preparedStatement.setString(17, record.syncid);
                            preparedStatement.setString(18, record.process_kafka);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(batchSize)
                                .withBatchIntervalMs(batchIntervalMs)
                                .withMaxRetries(sqlMaxRetries)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(jdbcUrl)
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername(dbUsername)
                                .withPassword(dbPassword)
                                .build()
                )
        ).name("MySQL Sink")
        .setParallelism(mySqlSinkParallelism);

        env.execute("Invoice Request Job");

    }

}