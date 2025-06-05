package com.thaihoc.sink;

import com.thaihoc.model.retry.InvoiceRetryRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;

public class InvoiceRetrySink extends RichSinkFunction<InvoiceRetryRecord> {
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final int maxRetries;
    private transient Connection connection;
    private transient PreparedStatement updateRetryStmt;
    private transient PreparedStatement insertRetryStmt;
    private transient PreparedStatement deleteRetryStmt;
    private transient PreparedStatement insertErrorStmt;
    public InvoiceRetrySink(String jdbcUrl, String username, String password, int maxRetries) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.maxRetries = maxRetries;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(false);

        String updateRetrySql = "UPDATE invoice_retry SET error_message = ?, error_code = ?, next_retry_time = CURRENT_TIMESTAMP + INTERVAL ? SECOND, retry_count = ?, state = 'PENDING' WHERE id = ? AND state = 'PROCESSING'";
        updateRetryStmt = connection.prepareStatement(updateRetrySql);

        String insertRetrySql = "INSERT INTO invoice_retry (sid, syncid, job, payload, next_retry_time, error_message, error_code, retry_count, state) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP + INTERVAL ? SECOND, ?, ?, ?, ?)";
        insertRetryStmt = connection.prepareStatement(insertRetrySql);

        String deleteRetrySql = "DELETE FROM invoice_retry WHERE id = ? AND state = 'PROCESSING'";
        deleteRetryStmt = connection.prepareStatement(deleteRetrySql);

        String insertErrorSql = "INSERT INTO invoice_error_log (payload, error_message, error_code, attempt, sid, syncid) VALUES (?, ?, ?, ?, ?, ?)";
        insertErrorStmt = connection.prepareStatement(insertErrorSql);
    }

    @Override
    public void invoke(InvoiceRetryRecord records, Context context) throws Exception {
        int retryCount = 0;
        while (retryCount <= maxRetries) {
            try {
                if (records.tag.equals("CREATE")) {
                    insertRetryRecord(records);
                } else if (records.tag.equals("UPDATE")) {
                updateRetryRecord(records);
                } else if (records.tag.equals("DELETE")) {
                    deleteRetryRecord(records);
                } else if (records.tag.equals("MAX_RETRY")) {
                    insertErrorRecord(records);
                }
                connection.commit();
                return;
            } catch (Exception e) {
                System.err.println("Error in InvoiceRetrySink: " + e.getMessage());
                retryCount++;
                try {
                    connection.rollback();
                } catch (SQLException rollbackException) {
                    System.err.println("Rollback failed: " + rollbackException.getMessage());
                }
                if (retryCount > maxRetries) {
                    throw new RuntimeException("Database operation failed after " + (maxRetries + 1) + " attempts", e);
                }
                // Wait before retry
                Thread.sleep(1000 * retryCount);
            }
        }
    }
    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
        updateRetryStmt.close();
        insertRetryStmt.close();
        deleteRetryStmt.close();
        insertErrorStmt.close();
    }

    private void insertRetryRecord(InvoiceRetryRecord record) throws Exception {
        insertRetryStmt.setString(1, record.sid);
        insertRetryStmt.setString(2, record.syncid);
        insertRetryStmt.setString(3, record.job);
        insertRetryStmt.setString(4, record.payload);
        insertRetryStmt.setLong(5, record.next_retry_time);
        insertRetryStmt.setString(6, record.error_message);
        insertRetryStmt.setString(7, record.error_code);
        insertRetryStmt.setInt(8, 0);
        insertRetryStmt.setString(9, "PENDING");
        insertRetryStmt.executeUpdate();
    }

    private void updateRetryRecord(InvoiceRetryRecord record) throws Exception {
        updateRetryStmt.setString(1, record.error_message);
        updateRetryStmt.setString(2, record.error_code);
        updateRetryStmt.setLong(3, record.next_retry_time);
        updateRetryStmt.setInt(4, record.retry_count);
        updateRetryStmt.setString(5, record.id.toString());
        updateRetryStmt.executeUpdate();
    }

    private void deleteRetryRecord(InvoiceRetryRecord record) throws Exception {
        deleteRetryStmt.setString(1, record.id.toString());
        deleteRetryStmt.executeUpdate();
    }

    private void insertErrorRecord(InvoiceRetryRecord record) throws Exception {
        insertErrorStmt.setString(1, record.payload);
        insertErrorStmt.setString(2, record.error_message);
        insertErrorStmt.setString(3, record.error_code);
        insertErrorStmt.setInt(4, record.retry_count - 1);
        insertErrorStmt.setString(5, record.sid);
        insertErrorStmt.setString(6, record.syncid);
        insertErrorStmt.executeUpdate();
        deleteRetryRecord(record);
    }
    
}
