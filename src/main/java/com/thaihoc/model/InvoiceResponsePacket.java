package com.thaihoc.model;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

public class InvoiceResponsePacket {
    public List<InvoiceResponseItem> inv_pack_res;

    public InvoiceResponsePacket() {}

    public static class InvoiceResponseItem {
        public String sid;
        public String sync_sid;
        public String message;
        public String status;
        public Integer code;
        public String res_resource;
        public JsonNode data;
        public String res_code;

        public InvoiceResponseItem() {}
    }
} 