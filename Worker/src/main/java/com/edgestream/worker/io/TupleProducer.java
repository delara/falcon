package com.edgestream.worker.io;

public interface TupleProducer {

    void notifyThatOperatorIsInWarmUpPhase(boolean inWarmUpPhase);
    void printWarmUpState();

}
