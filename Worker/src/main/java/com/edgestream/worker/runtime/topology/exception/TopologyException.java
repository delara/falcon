package com.edgestream.worker.runtime.topology.exception;


public class TopologyException extends RuntimeException {

    public TopologyException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
