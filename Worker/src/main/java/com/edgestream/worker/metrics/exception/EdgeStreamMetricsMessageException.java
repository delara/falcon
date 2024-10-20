package com.edgestream.worker.metrics.exception;

public class EdgeStreamMetricsMessageException extends RuntimeException {

    public EdgeStreamMetricsMessageException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}


