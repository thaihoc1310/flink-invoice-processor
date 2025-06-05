package com.thaihoc.source;

import com.thaihoc.model.response.AsyncInvOutRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class AsyncInvOutSource extends RichSourceFunction<AsyncInvOutRecord> {
    private volatile boolean isRunning = true;
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final long pollingIntervalMs;
    private final int fetchSize;
    private Connection connection;
    private volatile long lastProcessedId = 0; // Track last processed ID
    
    public AsyncInvOutSource(String jdbcUrl, String username, String password, long pollingIntervalMs, int fetchSize) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.pollingIntervalMs = pollingIntervalMs;
        this.fetchSize = fetchSize;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        
        // Initialize lastProcessedId from database or checkpoint
//        initializeLastProcessedId();
    }
    
    private void initializeLastProcessedId() throws SQLException {
        // Get the maximum ID that was already processed to avoid reprocessing
        String sql = "SELECT COALESCE(MAX(id), 0) as max_id FROM async_inv_out WHERE res_type = 2 AND state = 0";
        try (PreparedStatement stmt = connection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                lastProcessedId = rs.getLong("max_id");
                System.out.println("AsyncInvOut - Initialized lastProcessedId: " + lastProcessedId);
            }
        }
    }
    
    @Override
    public void run(SourceContext<AsyncInvOutRecord> ctx) throws Exception {
        while (isRunning) {
            // Use offset-based polling to avoid reading same records
            String sql = "SELECT * FROM async_inv_out WHERE res_type = 2 AND state = 0 AND id > ? ORDER BY id ASC LIMIT " + fetchSize;
            
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setLong(1, lastProcessedId);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    List<AsyncInvOutRecord> records = new ArrayList<>();
                    long maxIdInBatch = lastProcessedId;
                    
                    while (rs.next()) {
                        AsyncInvOutRecord record = new AsyncInvOutRecord();
                        record.id = rs.getInt("id");
                        record.tax_schema = rs.getString("tax_schema");
                        record.gdt_res = rs.getString("gdt_res");
                        record.sid = rs.getString("sid");
                        record.syncid = rs.getString("syncid");
                        record.retry = rs.getByte("retry");
                        record.state = rs.getByte("state");
                        record.group_id = rs.getByte("group_id");
                        record.created_date = rs.getTimestamp("created_date");
                        record.updated_date = rs.getTimestamp("updated_date");
                        record.res_type = rs.getByte("res_type");
                        record.process_kafka = rs.getString("process_kafka");
                        record.api_type = rs.getByte("api_type");
                        records.add(record);
                        
                        // Track the maximum ID in this batch
                        maxIdInBatch = Math.max(maxIdInBatch, record.id);
                    }
                    
                    // Emit records
                    for (AsyncInvOutRecord record : records) {
                        ctx.collect(record);
                    }
                    
                    // Update lastProcessedId only if we found records
                    if (!records.isEmpty()) {
                        lastProcessedId = maxIdInBatch;
                        System.out.println("AsyncInvOut - Processed " + records.size() + " records, updated lastProcessedId to: " + lastProcessedId);
                    }
                }
                
            } catch (Exception e) {
                // Log error but continue
                System.err.println("Error reading from async_inv_out: " + e.getMessage());
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
        if (connection != null) {
            connection.close();
        }
    }
} 