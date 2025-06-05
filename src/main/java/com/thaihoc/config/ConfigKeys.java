package com.thaihoc.config;

public final class ConfigKeys {
    static public final String DEFAULT_CONFIG_FILE_CLASSPATH = "application.properties";

    static public final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    static public final String KAFKA_TOPIC_CRT_REQUEST = "kafka.topic.crt.request";
    static public final String KAFKA_TOPIC_UPD_REQUEST = "kafka.topic.upd.request";
    static public final String KAFKA_TOPIC_DEL_REQUEST = "kafka.topic.del.request";
    static public final String KAFKA_TOPIC_REP_REQUEST = "kafka.topic.rep.request";
    static public final String KAFKA_TOPIC_ADJ_REQUEST = "kafka.topic.adj.request";
    static public final String KAFKA_TOPIC_CRT_RESPONSE = "kafka.topic.crt.response";
    static public final String KAFKA_TOPIC_UPD_RESPONSE = "kafka.topic.upd.response";
    static public final String KAFKA_TOPIC_DEL_RESPONSE = "kafka.topic.del.response";
    static public final String KAFKA_TOPIC_REP_RESPONSE = "kafka.topic.rep.response";
    static public final String KAFKA_TOPIC_ADJ_RESPONSE = "kafka.topic.adj.response";


    static public final String KAFKA_GROUP_ID_CRT_REQUEST = "kafka.group.id.crt.request";
    static public final String KAFKA_GROUP_ID_UPD_REQUEST = "kafka.group.id.upd.request";
    static public final String KAFKA_GROUP_ID_DEL_REQUEST = "kafka.group.id.del.request";
    static public final String KAFKA_GROUP_ID_REP_REQUEST = "kafka.group.id.rep.request";
    static public final String KAFKA_GROUP_ID_ADJ_REQUEST = "kafka.group.id.adj.request";


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

    static public final String MYSQL_POLLING_INTERVAL_MS = "mysql.polling.interval.ms";
    static public final String MYSQL_FETCH_SIZE = "mysql.fetch.size";

    static public final String FLINK_JOB_PARALLELISM = "flink.job.parallelism";
    
    // Invoice Request Job Parallelism Configuration
    static public final String REQUEST_KAFKA_SOURCE_PARALLELISM = "request.kafka.source.parallelism";
    static public final String REQUEST_PROCESSOR_PARALLELISM = "request.processor.parallelism";
    static public final String REQUEST_MYSQL_SINK_PARALLELISM = "request.mysql.sink.parallelism";
    
    // Invoice Response Job Parallelism Configuration
    static public final String RESPONSE_MYSQL_SOURCE_PARALLELISM = "response.mysql.source.parallelism";
    static public final String RESPONSE_BATCH_PROCESSOR_PARALLELISM = "response.batch.processor.parallelism";
    static public final String RESPONSE_KAFKA_SINK_PARALLELISM = "response.kafka.sink.parallelism";
    static public final String RESPONSE_TRANSACTIONAL_SINK_PARALLELISM = "response.transactional.sink.parallelism";
    
    static public final String APP_GROUP_ID_MAX_VALUE = "group.id.max.value";
    static public final String APP_MAX_RETRIES = "app.max.retries";
    static public final String APP_RETRY_INTERVAL_MS = "app.retry.interval.ms";

    static public final String RESPONSE_BATCH_SIZE = "response.batch.size";
    static public final String RESPONSE_BATCH_TIMEOUT_MS = "response.batch.timeout.ms";

    static public final String RETRY_MYSQL_POLLING_INTERVAL_MS = "retry.mysql.polling.interval.ms";
    static public final String RETRY_MYSQL_FETCH_SIZE = "retry.mysql.fetch.size";
    static public final String RETRY_MYSQL_SOURCE_PARALLELISM = "retry.mysql.source.parallelism";
    static public final String RETRY_MYSQL_SINK_PARALLELISM = "retry.mysql.sink.parallelism";
}


