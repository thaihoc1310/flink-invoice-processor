package com.thaihoc.model.retry;

import java.sql.Timestamp;

public class InvoiceErrorLogRecord {
    public Long id;
    public String payload;
    public String error_message;
    public String error_code;
    public byte attempt;
    public String sid;
    public String syncid;
    public Timestamp created_at;
}
