package com.edgestream.worker.runtime.task.exception;



public class EdgeStreamTaskRequestException extends RuntimeException {

    public EdgeStreamTaskRequestException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
