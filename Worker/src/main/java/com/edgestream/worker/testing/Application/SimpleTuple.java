package com.edgestream.worker.testing.Application;

import java.io.Serializable;

public class SimpleTuple implements Serializable {

    String payload;
    long createTime = 0l;


    public SimpleTuple() {
    }

    public SimpleTuple(String payload) {
        this.payload = payload;
        createTime = System.currentTimeMillis();
    }

    public String getPayload() {
        return payload;
    }

    public long getCreateTime() {
        return createTime;
    }
}
