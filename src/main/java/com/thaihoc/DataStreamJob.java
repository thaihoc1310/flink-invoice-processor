package com.thaihoc;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.io.InputStream;
import java.sql.Types;
import java.util.Properties;
import java.util.UUID;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = loadParameters(args);

        int defaultJobParallelism = params.getInt(ConfigKeys.FLINK_JOB_PARALLELISM, 1);
        env.setParallelism(defaultJobParallelism);

        KafkaSource<String> kafkaSource = createKafkaSource(params);
        DataStream<String> kafkaJsonStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        final int maxGroupIdValue = params.getInt(ConfigKeys.APP_GROUP_ID_MAX_VALUE, 4) + 1;

        DataStream<InvoiceMysqlRecord> mysqlRecordStream =
                kafkaJsonStream
                        .flatMap(new InvoiceTransformer(maxGroupIdValue))
                        .name("Transform Invoices");


        String jdbcUrl = params.getRequired(ConfigKeys.MYSQL_JDBC_URL);
        String dbUsername = params.getRequired(ConfigKeys.MYSQL_USERNAME);
        String dbPassword = params.getRequired(ConfigKeys.MYSQL_PASSWORD);
        String tableName = params.getRequired(ConfigKeys.MYSQL_TABLE_NAME);

        int batchSize = params.getInt(ConfigKeys.MYSQL_BATCH_SIZE, 100);
        long batchIntervalMs = params.getLong(ConfigKeys.MYSQL_BATCH_INTERVAL_MS, 2000L);
        int maxRetries = params.getInt(ConfigKeys.MYSQL_MAX_RETRIES, 3);

        String insertSql = "INSERT INTO " + tableName + " (" +
                "tax_schema, inv, api_type, res_type, fpt_einvoice_res_code, " +
                "fpt_einvoice_res_msg, fpt_einvoice_res_json, retry, state, group_id, " +
                "created_date, updated_date, callback_res_code, callback_res_msg, " +
                "callback_res_json, sid, syncid, process_kafka" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        mysqlRecordStream.addSink(
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

    private static KafkaSource<String> createKafkaSource(ParameterTool params) {
        String kafkaBootstrapServers = params.getRequired(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS);
        String kafkaTopic = params.getRequired(ConfigKeys.KAFKA_TOPIC_PRIMARY);
        String kafkaGroupId = params.get(ConfigKeys.KAFKA_GROUP_ID_PRIMARY, "default-flink-kafka-group-" + UUID.randomUUID());
        String kafkaSaslUsername = params.getRequired(ConfigKeys.KAFKA_SASL_USERNAME);
        String kafkaSaslPassword = params.getRequired(ConfigKeys.KAFKA_SASL_PASSWORD);
        String kafkaStartingOffsetsConfig = params.get(ConfigKeys.KAFKA_STARTING_OFFSETS, "LATEST").toUpperCase();

        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("security.protocol", "SASL_PLAINTEXT");
        kafkaConsumerProps.setProperty("sasl.mechanism", "PLAIN");
        String jaasConfig = String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                kafkaSaslUsername, kafkaSaslPassword);
        kafkaConsumerProps.setProperty("sasl.jaas.config", jaasConfig);

        OffsetsInitializer startingOffsets;
        switch (kafkaStartingOffsetsConfig) {
            case "EARLIEST":
                startingOffsets = OffsetsInitializer.earliest();
                break;
            case "LATEST":
                startingOffsets = OffsetsInitializer.latest();
                break;
            default:
                startingOffsets = OffsetsInitializer.committedOffsets();
                break;
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

}
