package com.thaihoc.model.response;

import java.sql.Timestamp;

public class AsyncInvOutRecord implements RecordInterface {
    public int id;
    public String tax_schema;
    public String gdt_res;
    public byte retry;
    public byte state;
    public byte group_id;
    public Timestamp created_date;
    public Timestamp updated_date;
    public String sid;
    public String syncid;
    public byte res_type;
    public String process_kafka;
    public byte api_type;

    public AsyncInvOutRecord() {}

    @Override
    public byte getApiType() {
        return api_type;
    }

    @Override
    public String getSid() {
        return sid;
    }

    @Override
    public String getSyncid() {
        return syncid;
    }
} 