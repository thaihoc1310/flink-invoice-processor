package com.thaihoc;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.io.InputStream;
import java.sql.Types;
import java.util.Properties;
import java.util.UUID;

public class DataStreamJob {
    public static final OutputTag<String> retry1OutputTag = new OutputTag<String>("retry-1-output"){};
    public static final OutputTag<String> retry2OutputTag = new OutputTag<String>("retry-2-output"){};
    public static final OutputTag<String> retry3OutputTag = new OutputTag<String>("retry-3-output"){};
    public static final OutputTag<String> dlqOutputTag = new OutputTag<String>("dlq-output"){};
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = loadParameters(args);

        final int defaultJobParallelism = params.getInt(ConfigKeys.FLINK_JOB_PARALLELISM, 1);
        env.setParallelism(defaultJobParallelism);

        KafkaSource<String> primarySource = createKafkaSource(params, ConfigKeys.KAFKA_TOPIC_PRIMARY, ConfigKeys.KAFKA_GROUP_ID_PRIMARY);
        KafkaSource<String> retry1Source = createKafkaSource(params, ConfigKeys.KAFKA_TOPIC_RETRY_1, ConfigKeys.KAFKA_GROUP_ID_RETRY_1);
        KafkaSource<String> retry2Source = createKafkaSource(params, ConfigKeys.KAFKA_TOPIC_RETRY_2, ConfigKeys.KAFKA_GROUP_ID_RETRY_2);
        KafkaSource<String> retry3Source = createKafkaSource(params, ConfigKeys.KAFKA_TOPIC_RETRY_3, ConfigKeys.KAFKA_GROUP_ID_RETRY_3);

        DataStream<String> primaryJsonStream = env.fromSource(primarySource, WatermarkStrategy.noWatermarks(), "Kafka Primary Source");
        DataStream<String> retry1JsonStream = env.fromSource(retry1Source, WatermarkStrategy.noWatermarks(), "Kafka Retry 1 Source").setParallelism(defaultJobParallelism/2);
        DataStream<String> retry2JsonStream = env.fromSource(retry2Source, WatermarkStrategy.noWatermarks(), "Kafka Retry 2 Source").setParallelism(defaultJobParallelism/2);
        DataStream<String> retry3JsonStream = env.fromSource(retry3Source, WatermarkStrategy.noWatermarks(), "Kafka Retry 3 Source").setParallelism(defaultJobParallelism/2);


        KafkaSink<String> retry1Sink = createKafkaSink(params, ConfigKeys.KAFKA_TOPIC_RETRY_1);
        KafkaSink<String> retry2Sink = createKafkaSink(params, ConfigKeys.KAFKA_TOPIC_RETRY_2);
        KafkaSink<String> retry3Sink = createKafkaSink(params, ConfigKeys.KAFKA_TOPIC_RETRY_3);
        KafkaSink<String> dlqSink = createKafkaSink(params, ConfigKeys.KAFKA_TOPIC_DLQ);

        final int maxGroupIdValue = params.getInt(ConfigKeys.APP_GROUP_ID_MAX_VALUE, 4) + 1;

        // Process primary topic stream
        SingleOutputStreamOperator<InvoiceMysqlRecord> processedPrimary = primaryJsonStream
                .process(new InvoiceProcessingRouter(0, maxGroupIdValue, retry1OutputTag, retry2OutputTag, retry3OutputTag, dlqOutputTag))
                .name("ProcessPrimaryInvoices")
                .setParallelism(defaultJobParallelism*2);
        processedPrimary.getSideOutput(retry1OutputTag).sinkTo(retry1Sink).name("SinkToRetry1").setParallelism(defaultJobParallelism/2);

        // Process retry topic 1 stream
        SingleOutputStreamOperator<InvoiceMysqlRecord> processedRetry1 = retry1JsonStream
                .process(new InvoiceProcessingRouter(1, maxGroupIdValue, retry1OutputTag, retry2OutputTag, retry3OutputTag, dlqOutputTag))
                .name("ProcessRetry1Invoices")
                .setParallelism(defaultJobParallelism/2);
        processedRetry1.getSideOutput(retry2OutputTag).sinkTo(retry2Sink).name("SinkToRetry2").setParallelism(defaultJobParallelism/2);

        // Process retry topic 2 stream
        SingleOutputStreamOperator<InvoiceMysqlRecord> processedRetry2 = retry2JsonStream
                .process(new InvoiceProcessingRouter(2, maxGroupIdValue, retry1OutputTag, retry2OutputTag, retry3OutputTag, dlqOutputTag))
                .name("ProcessRetry2Invoices")
                .setParallelism(defaultJobParallelism/2);
        processedRetry2.getSideOutput(retry3OutputTag).sinkTo(retry3Sink).name("SinkToRetry3").setParallelism(defaultJobParallelism/2);

        // Process retry topic 3 stream (final app-level retry)
        SingleOutputStreamOperator<InvoiceMysqlRecord> processedRetry3 = retry3JsonStream
                .process(new InvoiceProcessingRouter(3, maxGroupIdValue, retry1OutputTag, retry2OutputTag, retry3OutputTag, dlqOutputTag))
                .name("ProcessRetry3Invoices")
                .setParallelism(defaultJobParallelism/2);
        processedRetry3.getSideOutput(dlqOutputTag).sinkTo(dlqSink).name("SinkToDLQ").setParallelism(defaultJobParallelism/2);

        DataStream<InvoiceMysqlRecord> allSuccessfulRecords = processedPrimary
                .union(processedRetry1)
                .union(processedRetry2)
                .union(processedRetry3);

        final String jdbcUrl = params.getRequired(ConfigKeys.MYSQL_JDBC_URL);
        final String dbUsername = params.getRequired(ConfigKeys.MYSQL_USERNAME);
        final String dbPassword = params.getRequired(ConfigKeys.MYSQL_PASSWORD);
        final String tableName = params.getRequired(ConfigKeys.MYSQL_TABLE_NAME);

        final int batchSize = params.getInt(ConfigKeys.MYSQL_BATCH_SIZE, 100);
        final long batchIntervalMs = params.getLong(ConfigKeys.MYSQL_BATCH_INTERVAL_MS, 2000L);
        final int maxRetries = params.getInt(ConfigKeys.MYSQL_MAX_RETRIES, 3);

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
                                .withMaxRetries(maxRetries)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(jdbcUrl)
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername(dbUsername)
                                .withPassword(dbPassword)
                                .build()
                )
        ).name("MySQL Sink");

        env.execute("Flink Kafka to MySQL Invoice Processing Job");

    }

    private static ParameterTool loadParameters(String[] args) throws Exception {
        ParameterTool parameterToolFromArgs = ParameterTool.fromArgs(args);
        if (parameterToolFromArgs.has(ConfigKeys.CONFIG_FILE_PARAM)) {
            String configFilePath = parameterToolFromArgs.getRequired(ConfigKeys.CONFIG_FILE_PARAM);
            return ParameterTool.fromPropertiesFile(configFilePath);
        } else {
            try (InputStream inputStream = DataStreamJob.class.getClassLoader().getResourceAsStream(ConfigKeys.DEFAULT_CONFIG_FILE_CLASSPATH)) {
                return ParameterTool.fromPropertiesFile(inputStream);
            }
        }
    }

    private static KafkaSource<String> createKafkaSource(ParameterTool params, String topicConfigKey, String groupIdConfigKey) {
        final String kafkaBootstrapServers = params.getRequired(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS);
        final String kafkaTopic = params.getRequired(topicConfigKey);
        final String kafkaGroupId = params.get(groupIdConfigKey, "flink-group-" + topicConfigKey + "-" + UUID.randomUUID()); // More specific default group id
        final String kafkaSaslUsername = params.getRequired(ConfigKeys.KAFKA_SASL_USERNAME);
        final String kafkaSaslPassword = params.getRequired(ConfigKeys.KAFKA_SASL_PASSWORD);
        final String kafkaStartingOffsetsConfig = params.get(ConfigKeys.KAFKA_STARTING_OFFSETS, "LATEST").toUpperCase();

        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("security.protocol", "SASL_PLAINTEXT");
        kafkaConsumerProps.setProperty("sasl.mechanism", "PLAIN");
        String jaasConfig = String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                kafkaSaslUsername, kafkaSaslPassword);
        kafkaConsumerProps.setProperty("sasl.jaas.config", jaasConfig);

        OffsetsInitializer startingOffsets;
        switch (kafkaStartingOffsetsConfig) {
            case "EARLIEST": startingOffsets = OffsetsInitializer.earliest(); break;
            case "LATEST": startingOffsets = OffsetsInitializer.latest(); break;
            default: startingOffsets = OffsetsInitializer.committedOffsets(); break;
        }

        return KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics(kafkaTopic)
                .setGroupId(kafkaGroupId)
                .setStartingOffsets(startingOffsets)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(kafkaConsumerProps)
                .build();
    }

    private static KafkaSink<String> createKafkaSink(ParameterTool params, String topicConfigKey) {
        final String kafkaBootstrapServers = params.getRequired(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS);
        final String kafkaTopic = params.getRequired(topicConfigKey);
        final String kafkaSaslUsername = params.getRequired(ConfigKeys.KAFKA_SASL_USERNAME);
        final String kafkaSaslPassword = params.getRequired(ConfigKeys.KAFKA_SASL_PASSWORD);

        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty("security.protocol", "SASL_PLAINTEXT");
        kafkaProducerProps.setProperty("sasl.mechanism", "PLAIN");
        String jaasConfig = String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                kafkaSaslUsername, kafkaSaslPassword);
        kafkaProducerProps.setProperty("sasl.jaas.config", jaasConfig);

        return KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(kafkaTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setKafkaProducerConfig(kafkaProducerProps)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
