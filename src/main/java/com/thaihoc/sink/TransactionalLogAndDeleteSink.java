package com.thaihoc.sink;
import com.thaihoc.model.AsyncInvInRecord;
import com.thaihoc.model.AsyncInvOutRecord;
import com.thaihoc.model.AsyncInvSuccLogRecord;
import com.thaihoc.model.RecordInterface;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.util.List;

public class TransactionalLogAndDeleteSink  extends RichSinkFunction<List<RecordInterface>> {
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final int maxRetries;
    private transient Connection connection;
    
    public TransactionalLogAndDeleteSink(String jdbcUrl, String username, String password, int maxRetries) {
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
        connection.setAutoCommit(false); // Enable transaction mode
    }
    
    @Override
    public void invoke(List<RecordInterface> records, Context context) throws Exception {
        if (records == null || records.isEmpty()) {
            return;
        }
        
        int retryCount = 0;
        while (retryCount <= maxRetries) {
            try {
                processRecordsInTransaction(records);
                return;
            } catch (SQLException e) {
                retryCount++;
                System.err.println("Database operation failed (attempt " + retryCount + "/" + (maxRetries + 1) + "): " + e.getMessage());
                
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
    
    private void processRecordsInTransaction(List<RecordInterface> records) throws SQLException {
        String insertLogSql = "INSERT INTO async_inv_succ_log (" +
                "tax_schema, api_type, res_type, fpt_einvoice_res_code, fpt_einvoice_res_msg, " +
                "retry, group_id, created_date, updated_date, callback_res_code, callback_res_msg, " +
                "sid, syncid, gdt_res" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        String deleteInvInSql = "DELETE FROM async_inv_in WHERE id = ?";
        String deleteInvOutSql = "DELETE FROM async_inv_out WHERE id = ?";
        
        try (PreparedStatement insertStmt = connection.prepareStatement(insertLogSql);
             PreparedStatement deleteInvInStmt = connection.prepareStatement(deleteInvInSql);
             PreparedStatement deleteInvOutStmt = connection.prepareStatement(deleteInvOutSql)) {
            
            for (RecordInterface record : records) {
                if (record instanceof AsyncInvInRecord) {
                    AsyncInvInRecord invInRecord = (AsyncInvInRecord) record;
                    
                    // Insert into log table
                    AsyncInvSuccLogRecord logRecord = createLogRecordForInvIn(invInRecord);
                    setInsertLogParameters(insertStmt, logRecord);
                    insertStmt.addBatch();
                    
                    // Delete from inv_in table
                    deleteInvInStmt.setInt(1, invInRecord.id);
                    deleteInvInStmt.addBatch();
                    
                } else if (record instanceof AsyncInvOutRecord) {
                    AsyncInvOutRecord invOutRecord = (AsyncInvOutRecord) record;
                    
                    AsyncInvSuccLogRecord logRecord = createLogRecordForInvOut(invOutRecord);
                    setInsertLogParameters(insertStmt, logRecord);
                    insertStmt.addBatch();
                    // Delete from inv_out table (no log entry for inv_out)
                    deleteInvOutStmt.setInt(1, invOutRecord.id);
                    deleteInvOutStmt.addBatch();
                }
            }
            
            // Execute all operations in transaction
            insertStmt.executeBatch();
            deleteInvInStmt.executeBatch();
            deleteInvOutStmt.executeBatch();
            
            // Commit transaction
            connection.commit();
            
        } catch (SQLException e) {
            throw e;
        }
    }
    
    private void setInsertLogParameters(PreparedStatement stmt, AsyncInvSuccLogRecord record) throws SQLException {
        stmt.setString(1, record.tax_schema);
        stmt.setByte(2, record.api_type);
        stmt.setByte(3, record.res_type);
        stmt.setString(4, record.fpt_einvoice_res_code);
        stmt.setString(5, record.fpt_einvoice_res_msg);
        stmt.setByte(6, record.retry);
        stmt.setByte(7, record.group_id);
        stmt.setTimestamp(8, record.created_date);
        stmt.setNull(9, Types.TIMESTAMP);
        stmt.setString(10, record.callback_res_code);
        stmt.setString(11, record.callback_res_msg);
        stmt.setString(12, record.sid);
        stmt.setString(13, record.syncid);
        stmt.setString(14, record.gdt_res);
    }

    
    private AsyncInvSuccLogRecord createLogRecordForInvIn(AsyncInvInRecord record) {
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
        logRecord.gdt_res = null;
        return logRecord;
    }

    private AsyncInvSuccLogRecord createLogRecordForInvOut(AsyncInvOutRecord record) {
        AsyncInvSuccLogRecord logRecord = new AsyncInvSuccLogRecord();
        logRecord.tax_schema = record.tax_schema;
        logRecord.api_type = record.api_type;
        logRecord.res_type = record.res_type;
        logRecord.fpt_einvoice_res_code = null;
        logRecord.fpt_einvoice_res_msg = null;
        logRecord.retry = record.retry;
        logRecord.group_id = record.group_id;
        logRecord.created_date = new Timestamp(System.currentTimeMillis());
        logRecord.updated_date = null;
        logRecord.callback_res_code = null;
        logRecord.callback_res_msg = null;
        logRecord.sid = record.sid;
        logRecord.syncid = record.syncid;
        logRecord.gdt_res = record.gdt_res;
        return logRecord;
    }
    
    
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                System.err.println("Error closing database connection: " + e.getMessage());
            }
        }
    }
}
