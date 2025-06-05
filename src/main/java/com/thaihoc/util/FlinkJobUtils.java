package com.thaihoc.util;

import com.thaihoc.config.ConfigKeys;
import com.thaihoc.job.InvoiceRequest;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.InputStream;
import java.util.Properties;

public class FlinkJobUtils {
    public static ParameterTool loadParameters(String[] args) throws Exception {
        ParameterTool parameterToolFromArgs = ParameterTool.fromArgs(args);
        
        ParameterTool parameterToolFromProperties;
        try (InputStream inputStream = InvoiceRequest.class.getClassLoader().getResourceAsStream(ConfigKeys.DEFAULT_CONFIG_FILE_CLASSPATH)) {
            parameterToolFromProperties = ParameterTool.fromPropertiesFile(inputStream);
        }

        return parameterToolFromProperties.mergeWith(parameterToolFromArgs);
    }

    public static KafkaSource<String> createKafkaSource(ParameterTool params, String topicConfigKey, String groupIdConfigKey) {
        final String kafkaBootstrapServers = params.getRequired(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS);
        final String kafkaTopic = params.getRequired(topicConfigKey);
        final String kafkaGroupId = params.get(groupIdConfigKey, "flink-invoice-processor-group-" + topicConfigKey);
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
            case "COMMITTED_OFFSETS":
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

    public static KafkaSink<String> createKafkaSink(ParameterTool params, String topicConfigKey) {
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