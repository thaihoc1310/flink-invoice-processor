package com.thaihoc.model.retry;

import java.sql.Timestamp;

public class InvoiceRetryRecord {
    public Long id;
    public String payload;
    public String error_message;
    public String error_code;
    public byte retry_count;
    public String state; // PENDING, PROCESSING
    public String sid;
    public String syncid;
    public String job; //REQUEST, RESPONSE
    public long next_retry_time;
    public Timestamp created_at;
    public Timestamp updated_at;
    public String tag;
} 