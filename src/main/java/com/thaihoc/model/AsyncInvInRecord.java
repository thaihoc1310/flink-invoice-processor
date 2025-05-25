package com.thaihoc.model;

import java.sql.Timestamp;

public class AsyncInvInRecord implements RecordInterface {
    public int id;
    public String tax_schema;
    // public String inv;
    public byte api_type;
    public byte res_type;
    public String fpt_einvoice_res_code;
    public String fpt_einvoice_res_msg;
    public String fpt_einvoice_res_json;
    public byte retry;
    public byte state;
    public byte group_id;
    // public Timestamp created_date;
    // public Timestamp updated_date;
    public String callback_res_code;
    public String callback_res_msg;
    // public String callback_res_json;
    public String sid;
    public String syncid;
    // public String process_kafka;

    public AsyncInvInRecord() {}

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