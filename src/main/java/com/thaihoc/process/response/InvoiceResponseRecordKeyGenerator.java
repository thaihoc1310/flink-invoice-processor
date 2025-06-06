package com.thaihoc.process.response;

import com.thaihoc.model.response.AsyncInvInRecord;
import com.thaihoc.model.response.AsyncInvOutRecord;
import com.thaihoc.model.response.RecordInterface;

public class InvoiceResponseRecordKeyGenerator {
    
    public String generateRecordKey(RecordInterface record) {
        if (record instanceof AsyncInvInRecord) {
            AsyncInvInRecord invIn = (AsyncInvInRecord) record;
            return "InvIn_" + invIn.id + "_" + invIn.sid + "_" + invIn.syncid;
        } else if (record instanceof AsyncInvOutRecord) {
            AsyncInvOutRecord invOut = (AsyncInvOutRecord) record;
            return "InvOut_" + invOut.id + "_" + invOut.sid + "_" + invOut.syncid;
        }
        return record.getSid() + "_" + record.getSyncid();
    }
} 