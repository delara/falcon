package com.edgestream.client;

public class EdgeStreamClientException extends RuntimeException {

    public EdgeStreamClientException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
