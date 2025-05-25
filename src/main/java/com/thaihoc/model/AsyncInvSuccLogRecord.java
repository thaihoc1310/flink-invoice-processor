package com.thaihoc.model;

import java.sql.Timestamp;

public class AsyncInvSuccLogRecord  {
    public String tax_schema;
    public byte api_type;
    public byte res_type;
    public String fpt_einvoice_res_code;
    public String fpt_einvoice_res_msg;
    public byte retry;
    public byte group_id;
    public Timestamp created_date;
    public Timestamp updated_date;
    public String callback_res_code;
    public String callback_res_msg;
    public String sid;
    public String syncid;
    public String gdt_res;

    public AsyncInvSuccLogRecord() {}
}
