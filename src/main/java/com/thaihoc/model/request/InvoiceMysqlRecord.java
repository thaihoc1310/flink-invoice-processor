package com.thaihoc.model.request;
import java.sql.Timestamp;

public class InvoiceMysqlRecord {
    public String tax_schema;
    public String inv;
    public byte api_type;
    public Byte res_type;
    public String fpt_einvoice_res_code;
    public String fpt_einvoice_res_msg;
    public String fpt_einvoice_res_json;
    public byte retry;
    public byte state;
    public byte group_id;
    public Timestamp created_date;
    public Timestamp updated_date;
    public String callback_res_code;
    public String callback_res_msg;
    public String callback_res_json;
    public String sid;
    public String syncid;
    public String process_kafka;
}