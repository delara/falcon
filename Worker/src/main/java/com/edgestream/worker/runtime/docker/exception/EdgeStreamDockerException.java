package com.edgestream.worker.runtime.docker.exception;



public class EdgeStreamDockerException extends RuntimeException {

    public EdgeStreamDockerException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
