package com.edgestream.worker.runtime.reconfiguration.exception;

public class EdgeStreamReconfigurationException extends RuntimeException {

    public EdgeStreamReconfigurationException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
