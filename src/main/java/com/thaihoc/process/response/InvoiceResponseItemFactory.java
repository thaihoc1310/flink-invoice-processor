package com.thaihoc.process.response;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thaihoc.model.AsyncInvInRecord;
import com.thaihoc.model.AsyncInvOutRecord;
import com.thaihoc.model.InvoiceResponsePacket;
import com.thaihoc.model.RecordInterface;

public class InvoiceResponseItemFactory {
    private final ObjectMapper objectMapper;

    public InvoiceResponseItemFactory(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public InvoiceResponsePacket.InvoiceResponseItem createResponseItem(RecordInterface record) throws Exception {
        if (record instanceof AsyncInvInRecord) {
            return createInvInResponseItem((AsyncInvInRecord) record);
        } else if (record instanceof AsyncInvOutRecord) {
            return createInvOutResponseItem((AsyncInvOutRecord) record);
        }
        return null;
    }

    private InvoiceResponsePacket.InvoiceResponseItem createInvInResponseItem(AsyncInvInRecord record) throws Exception {
        InvoiceResponsePacket.InvoiceResponseItem item = new InvoiceResponsePacket.InvoiceResponseItem();
        item.sid = record.getSid();
        item.sync_sid = record.getSyncid();
        item.res_code = record.fpt_einvoice_res_code;
        
        if (record.fpt_einvoice_res_msg == null) {
            item.message = "Tạo mới thành công";
            item.status = "success";
        } else {
            item.message = record.fpt_einvoice_res_msg;
            item.status = "error";
        }
        
        item.res_resource = "fpt";
        item.code = null;
        
        if (record.fpt_einvoice_res_json != null) {
            item.data = objectMapper.readTree(record.fpt_einvoice_res_json);
        }
        
        return item;
    }

    private InvoiceResponsePacket.InvoiceResponseItem createInvOutResponseItem(AsyncInvOutRecord record) throws Exception {
        InvoiceResponsePacket.InvoiceResponseItem item = new InvoiceResponsePacket.InvoiceResponseItem();
        item.sid = record.getSid();
        item.sync_sid = record.getSyncid();
        item.message = null;
        item.status = null;
        item.code = null;
        item.res_code = null;
        item.res_resource = "gdt";
        
        if (record.gdt_res != null) {
            item.data = objectMapper.readTree(record.gdt_res);
        }
        
        return item;
    }
} 