package com.edgestream.worker.metrics.exception;


public class EdgeStreamHardwareStatsException extends RuntimeException {

    public EdgeStreamHardwareStatsException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
