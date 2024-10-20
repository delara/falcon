package com.edgestream.worker.io;

public interface TupleConsumer {

    void  notifyThatOperatorIsInWarmUpPhase(boolean inWarmUpPhase);
    void  printWarmUpState();
}
