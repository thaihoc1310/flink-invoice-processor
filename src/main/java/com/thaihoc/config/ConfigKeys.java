package com.thaihoc.config;

public final class ConfigKeys {
    static public final String CONFIG_FILE_PARAM = "config.file";
    static public final String DEFAULT_CONFIG_FILE_CLASSPATH = "application.properties";

    static public final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    static public final String KAFKA_TOPIC_CRT_REQUEST = "kafka.topic.crt.request";
    static public final String KAFKA_TOPIC_UPD_REQUEST = "kafka.topic.upd.request";
    static public final String KAFKA_TOPIC_DEL_REQUEST = "kafka.topic.del.request";
    static public final String KAFKA_TOPIC_REP_REQUEST = "kafka.topic.rep.request";
    static public final String KAFKA_TOPIC_ADJ_REQUEST = "kafka.topic.adj.request";

    static public final String KAFKA_TOPIC_RETRY_1 = "kafka.topic.retry.1";
    static public final String KAFKA_TOPIC_RETRY_2 = "kafka.topic.retry.2";
    static public final String KAFKA_TOPIC_RETRY_3 = "kafka.topic.retry.3";
    static public final String KAFKA_TOPIC_DLQ = "kafka.topic.dlq";

    static public final String KAFKA_GROUP_ID_CRT_REQUEST = "kafka.group.id.crt.request";
    static public final String KAFKA_GROUP_ID_UPD_REQUEST = "kafka.group.id.upd.request";
    static public final String KAFKA_GROUP_ID_DEL_REQUEST = "kafka.group.id.del.request";
    static public final String KAFKA_GROUP_ID_REP_REQUEST = "kafka.group.id.rep.request";
    static public final String KAFKA_GROUP_ID_ADJ_REQUEST = "kafka.group.id.adj.request";

    static public final String KAFKA_GROUP_ID_RETRY_1 = "kafka.group.id.retry.1";
    static public final String KAFKA_GROUP_ID_RETRY_2 = "kafka.group.id.retry.2";
    static public final String KAFKA_GROUP_ID_RETRY_3 = "kafka.group.id.retry.3";

    static public final String KAFKA_SASL_USERNAME = "kafka.sasl.username";
    static public final String KAFKA_SASL_PASSWORD = "kafka.sasl.password";
    static public final String KAFKA_STARTING_OFFSETS = "kafka.starting.offsets";

    static public final String MYSQL_JDBC_URL = "mysql.jdbc.url";
    static public final String MYSQL_USERNAME = "mysql.username";
    static public final String MYSQL_PASSWORD = "mysql.password";
    static public final String MYSQL_TABLE_NAME = "mysql.table.name";
    static public final String MYSQL_BATCH_SIZE = "mysql.batch.size";
    static public final String MYSQL_BATCH_INTERVAL_MS = "mysql.batch.interval.ms";
    static public final String MYSQL_MAX_RETRIES = "mysql.max.retries";

    static public final String APP_GROUP_ID_MAX_VALUE = "group.id.max.value";

    static public final String FLINK_JOB_PARALLELISM = "flink.job.parallelism";

    static public final int MAX_KAFKA_RETRIES = 3;
}


