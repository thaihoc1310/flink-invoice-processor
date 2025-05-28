package com.thaihoc.source;

import com.thaihoc.model.AsyncInvInRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class AsyncInvInSource extends RichSourceFunction<AsyncInvInRecord> {
    private volatile boolean isRunning = true;
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final long pollingIntervalMs;
    private final int fetchSize;
    private Connection connection;
    private volatile long lastProcessedId = 0; // Track last processed ID
    
    public AsyncInvInSource(String jdbcUrl, String username, String password, long pollingIntervalMs, int fetchSize) {
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
        String sql = "SELECT COALESCE(MAX(id), 0) as max_id FROM async_inv_in WHERE res_type = 2 AND state = 4";
        try (PreparedStatement stmt = connection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                lastProcessedId = rs.getLong("max_id");
                System.out.println("Initialized lastProcessedId: " + lastProcessedId);
            }
        }
    }
    
    @Override
    public void run(SourceContext<AsyncInvInRecord> ctx) throws Exception {
        while (isRunning) {
            // Use offset-based polling to avoid reading same records
            String sql = "SELECT * FROM async_inv_in WHERE res_type = 2 AND state = 4 AND id > ? ORDER BY id ASC LIMIT " + fetchSize;
            
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setLong(1, lastProcessedId);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    List<AsyncInvInRecord> records = new ArrayList<>();
                    long maxIdInBatch = lastProcessedId;
                    
                    while (rs.next()) {
                        AsyncInvInRecord record = new AsyncInvInRecord();
                        record.id = rs.getInt("id");
                        record.tax_schema = rs.getString("tax_schema");
                        record.api_type = rs.getByte("api_type");
                        record.fpt_einvoice_res_code = rs.getString("fpt_einvoice_res_code");
                        record.fpt_einvoice_res_msg = rs.getString("fpt_einvoice_res_msg");
                        record.fpt_einvoice_res_json = rs.getString("fpt_einvoice_res_json");
                        record.sid = rs.getString("sid");
                        record.syncid = rs.getString("syncid");
                        record.retry = rs.getByte("retry");
                        record.state = rs.getByte("state");
                        record.group_id = rs.getByte("group_id");
                        record.callback_res_code = rs.getString("callback_res_code");
                        record.callback_res_msg = rs.getString("callback_res_msg");
                        record.res_type = rs.getByte("res_type");
                        records.add(record);
                        
                        // Track the maximum ID in this batch
                        maxIdInBatch = Math.max(maxIdInBatch, record.id);
                    }
                    
                    // Emit records
                    for (AsyncInvInRecord record : records) {
                        ctx.collect(record);
                    }
                    
                    // Update lastProcessedId only if we found records
                    if (!records.isEmpty()) {
                        lastProcessedId = maxIdInBatch;
                    }
                }
                
            } catch (Exception ignored) {
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