package com.edgestream.worker.runtime.reconfiguration.state;

import java.io.Serializable;

public class AtomicKey implements Serializable {

    private String key;
    private String value;

    public AtomicKey() {

    }

    public AtomicKey(String key, String value) {

        this.key = key;
        this.value = value;

    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
