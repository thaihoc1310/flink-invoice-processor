package com.thaihoc.source;

import com.thaihoc.model.retry.InvoiceRetryRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class InvoiceRetrySource extends RichSourceFunction<InvoiceRetryRecord> {
    private volatile boolean isRunning = true;
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String job;
    private final long pollingIntervalMs;
    private final int fetchSize;
    private Connection connection;
    private PreparedStatement updateStateStmt;
    
    public InvoiceRetrySource(String jdbcUrl, String username, String password, String job,
                            long pollingIntervalMs, int fetchSize) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.job = job;
        this.pollingIntervalMs = pollingIntervalMs;
        this.fetchSize = fetchSize;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(false);
        
        // Prepare statement to update state to PROCESSING
        String updateSql = "UPDATE invoice_retry SET state = 'PROCESSING', updated_at = CURRENT_TIMESTAMP WHERE id = ?";
        updateStateStmt = connection.prepareStatement(updateSql);
    }
    
    @Override
    public void run(SourceContext<InvoiceRetryRecord> ctx) throws Exception {
        while (isRunning) {
            // Select records that are ready for retry
            String sql = "SELECT * FROM invoice_retry WHERE state = 'PENDING' AND next_retry_time <= CURRENT_TIMESTAMP AND job = ? ORDER BY next_retry_time ASC LIMIT ? ";
            
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setString(1, job);
                stmt.setInt(2, fetchSize);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    List<InvoiceRetryRecord> records = new ArrayList<>();
                    List<Long> recordIds = new ArrayList<>();
                    
                    while (rs.next()) {
                        InvoiceRetryRecord record = new InvoiceRetryRecord();
                        record.id = rs.getLong("id");
                        record.payload = rs.getString("payload");
                        record.error_message = rs.getString("error_message");
                        record.error_code = rs.getString("error_code");
                        record.retry_count = rs.getByte("retry_count");
                        record.state = rs.getString("state");
                        record.sid = rs.getString("sid");
                        record.syncid = rs.getString("syncid");
                        record.created_at = rs.getTimestamp("created_at");
                        record.updated_at = rs.getTimestamp("updated_at");
                        
                        records.add(record);
                        recordIds.add(record.id);
                    }
                    
                    // Update state to PROCESSING for selected records
                    if (!records.isEmpty()) {
                        for (Long id : recordIds) {
                            updateStateStmt.setLong(1, id);
                            updateStateStmt.executeUpdate();
                        }
                        
                        // Emit records
                        for (InvoiceRetryRecord record : records) {
                            record.state = "PROCESSING"; // Update local state
                            ctx.collect(record);
                        }
                    }
                    connection.commit();
                }
                
            } catch (Exception e) {
                connection.rollback();
                System.err.println("Error in InvoiceRetrySource: " + e.getMessage());
            }
            
            // Wait before next poll
            Thread.sleep(pollingIntervalMs);
        }
    }
    
    @Override
    public void cancel() {
        isRunning = false;
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        if (updateStateStmt != null) {
            updateStateStmt.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
} 