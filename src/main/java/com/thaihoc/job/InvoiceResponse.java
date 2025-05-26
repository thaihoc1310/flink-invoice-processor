package com.thaihoc.job;

import com.thaihoc.config.ConfigKeys;
import com.thaihoc.model.AsyncInvInRecord;
import com.thaihoc.model.AsyncInvOutRecord;
import com.thaihoc.model.AsyncInvSuccLogRecord;
import com.thaihoc.model.RecordInterface;
import com.thaihoc.process.BatchResponseProcessor;
import com.thaihoc.source.AsyncInvInSource;
import com.thaihoc.source.AsyncInvOutSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.*;

import static com.thaihoc.util.FlinkJobUtils.createKafkaSink;
import static com.thaihoc.util.FlinkJobUtils.loadParameters;

public class InvoiceResponse {
    
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = loadParameters(args);

        final int defaultJobParallelism = params.getInt(ConfigKeys.FLINK_JOB_PARALLELISM, 1);
//        final int mySqlSinkParallelism = params.getInt(ConfigKeys.MYSQL_SINK_PARALLELISM, 1);
        env.setParallelism(defaultJobParallelism);

        // Database configuration
        final String jdbcUrl = params.getRequired(ConfigKeys.MYSQL_JDBC_URL);
        final String dbUsername = params.getRequired(ConfigKeys.MYSQL_USERNAME);
        final String dbPassword = params.getRequired(ConfigKeys.MYSQL_PASSWORD);
        final long pollingIntervalMs = params.getLong(ConfigKeys.MYSQL_POLLING_INTERVAL_MS, 5000);
        final int fetchSize = params.getInt(ConfigKeys.MYSQL_FETCH_SIZE, 1000);
        final int batchSize = params.getInt(ConfigKeys.MYSQL_BATCH_SIZE, 2000);
        final long batchIntervalMs = params.getLong(ConfigKeys.MYSQL_BATCH_INTERVAL_MS, 5000);
        final int sqlMaxRetries = params.getInt(ConfigKeys.MYSQL_MAX_RETRIES, 3);

        // JDBC Connection Options
        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername(dbUsername)
                .withPassword(dbPassword)
                .build();

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
        
        DataStream<RecordInterface> unionInvRecords = invInStream.map(record -> (RecordInterface) record)
                .union(invOutStream.map(record -> (RecordInterface) record));
        BatchResponseProcessor batchProcessorUnion = new BatchResponseProcessor(responseBatchSize, responseBatchTimeoutMs);
        SingleOutputStreamOperator<String> processedBatchesUnion = unionInvRecords
                .keyBy(new KeySelector<RecordInterface, Byte>() {
                    @Override
                    public Byte getKey(RecordInterface record) throws Exception {
                        return record.getApiType();
                    }
                })
                .process(batchProcessorUnion)
                .name("Batch Response Processing (Union)");

        processedBatchesUnion.getSideOutput(BatchResponseProcessor.CRT_OUTPUT_TAG)
                .sinkTo(crtResponseSink).name("Sink CRT Batch Responses (InvIn + InvOut)");
        processedBatchesUnion.getSideOutput(BatchResponseProcessor.UPD_OUTPUT_TAG)
                .sinkTo(updResponseSink).name("Sink UPD Batch Responses (InvIn + InvOut)");
        processedBatchesUnion.getSideOutput(BatchResponseProcessor.DEL_OUTPUT_TAG)
                .sinkTo(delResponseSink).name("Sink DEL Batch Responses (InvIn + InvOut)");
        processedBatchesUnion.getSideOutput(BatchResponseProcessor.REP_OUTPUT_TAG)
                .sinkTo(repResponseSink).name("Sink REP Batch Responses (InvIn + InvOut)");
        processedBatchesUnion.getSideOutput(BatchResponseProcessor.ADJ_OUTPUT_TAG)
                .sinkTo(adjResponseSink).name("Sink ADJ Batch Responses (InvIn + InvOut)");

        // Insert invIn records to async_inv_succ_log
        DataStream<AsyncInvSuccLogRecord> succLogRecords = invInStream.map(new MapFunction<AsyncInvInRecord, AsyncInvSuccLogRecord>() {
            @Override
            public AsyncInvSuccLogRecord map(AsyncInvInRecord record) throws Exception {
                AsyncInvSuccLogRecord logRecord = new AsyncInvSuccLogRecord();
                logRecord.tax_schema = record.tax_schema;
                logRecord.api_type = record.api_type;
                logRecord.res_type = record.res_type;
                logRecord.fpt_einvoice_res_code = record.fpt_einvoice_res_code;
                logRecord.fpt_einvoice_res_msg = record.fpt_einvoice_res_msg;
                logRecord.retry = record.retry;
                logRecord.group_id = record.group_id;
                logRecord.created_date = new Timestamp(System.currentTimeMillis());
                logRecord.updated_date = null;
                logRecord.callback_res_code = record.callback_res_code;
                logRecord.callback_res_msg = record.callback_res_msg;
                logRecord.sid = record.sid;
                logRecord.syncid = record.syncid;
                logRecord.gdt_res = record.fpt_einvoice_res_json;
                return logRecord;
            }
        }).name("Transform to SuccLog Records");

        // Sink to async_inv_succ_log table
        String insertSuccLogSql = "INSERT INTO async_inv_succ_log (" +
                "tax_schema, api_type, res_type, fpt_einvoice_res_code, fpt_einvoice_res_msg, " +
                "retry, group_id, created_date, updated_date, callback_res_code, callback_res_msg, " +
                "sid, syncid, gdt_res" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        succLogRecords.addSink(
                JdbcSink.sink(
                        insertSuccLogSql,
                        (preparedStatement, record) -> {
                            preparedStatement.setString(1, record.tax_schema);
                            preparedStatement.setByte(2, record.api_type);
                            preparedStatement.setByte(3, record.res_type);
                            preparedStatement.setString(4, record.fpt_einvoice_res_code);
                            preparedStatement.setString(5, record.fpt_einvoice_res_msg);
                            preparedStatement.setByte(6, record.retry);
                            preparedStatement.setByte(7, record.group_id);
                            preparedStatement.setTimestamp(8, record.created_date);
                            preparedStatement.setNull(9, Types.TIMESTAMP);
                            preparedStatement.setString(10, record.callback_res_code);
                            preparedStatement.setString(11, record.callback_res_msg);
                            preparedStatement.setString(12, record.sid);
                            preparedStatement.setString(13, record.syncid);
                            preparedStatement.setString(14, record.gdt_res);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(batchSize)
                                .withBatchIntervalMs(batchIntervalMs)
                                .withMaxRetries(sqlMaxRetries)
                                .build(),
                        connectionOptions
                )
        ).name("Insert to async_inv_succ_log");

        // Delete processed records from async_inv_in
        String deleteInvInSql = "DELETE FROM async_inv_in WHERE res_type = 2 AND state = 4 AND id = ?";
        invInStream.addSink(
                JdbcSink.sink(
                        deleteInvInSql,
                        (preparedStatement, record) -> {
                            preparedStatement.setInt(1, record.id);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(batchSize)
                                .withBatchIntervalMs(batchIntervalMs)
                                .withMaxRetries(sqlMaxRetries)
                                .build(),
                        connectionOptions
                )
        ).name("Delete from async_inv_in");

        // Delete processed records from async_inv_out
        String deleteInvOutSql = "DELETE FROM async_inv_out WHERE res_type = 2 AND state = 0 AND id = ?";
        invOutStream.addSink(
                JdbcSink.sink(
                        deleteInvOutSql,
                        (preparedStatement, record) -> {
                            preparedStatement.setInt(1, record.id);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(batchSize)
                                .withBatchIntervalMs(batchIntervalMs)
                                .withMaxRetries(sqlMaxRetries)
                                .build(),
                        connectionOptions
                )
        ).name("Delete from async_inv_out");

        env.execute("Invoice Response Job");
    }
}

