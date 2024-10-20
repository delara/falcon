package com.edgestream.worker.operator;

public interface SingleTupleSpout {

    void installSpout(Spout spout);
    void activateSpout(Spout spout);
    void deactivateSpout(Spout spout);

}
