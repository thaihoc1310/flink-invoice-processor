package com.thaihoc;

public final class ConfigKeys {
    static final String CONFIG_FILE_PARAM = "config.file";
    static final String DEFAULT_CONFIG_FILE_CLASSPATH = "application.properties";

    static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    static final String KAFKA_TOPIC_PRIMARY = "kafka.topic.primary";
    static final String KAFKA_TOPIC_RETRY_1 = "kafka.topic.retry.1";
    static final String KAFKA_TOPIC_RETRY_2 = "kafka.topic.retry.2";
    static final String KAFKA_TOPIC_RETRY_3 = "kafka.topic.retry.3";
    static final String KAFKA_TOPIC_DLQ = "kafka.topic.dlq";

    static final String KAFKA_GROUP_ID_PRIMARY = "kafka.group.id.primary";
    static final String KAFKA_GROUP_ID_RETRY_1 = "kafka.group.id.retry.1";
    static final String KAFKA_GROUP_ID_RETRY_2 = "kafka.group.id.retry.2";
    static final String KAFKA_GROUP_ID_RETRY_3 = "kafka.group.id.retry.3";

    static final String KAFKA_SASL_USERNAME = "kafka.sasl.username";
    static final String KAFKA_SASL_PASSWORD = "kafka.sasl.password";
    static final String KAFKA_STARTING_OFFSETS = "kafka.starting.offsets";

    static final String MYSQL_JDBC_URL = "mysql.jdbc.url";
    static final String MYSQL_USERNAME = "mysql.username";
    static final String MYSQL_PASSWORD = "mysql.password";
    static final String MYSQL_TABLE_NAME = "mysql.table.name";
    static final String MYSQL_BATCH_SIZE = "mysql.batch.size";
    static final String MYSQL_BATCH_INTERVAL_MS = "mysql.batch.interval.ms";
    static final String MYSQL_MAX_RETRIES = "mysql.max.retries";

    static final String APP_GROUP_ID_MAX_VALUE = "group.id.max.value";

    static final String FLINK_JOB_PARALLELISM = "flink.job.parallelism";

    static final int MAX_KAFKA_RETRIES = 3;
}