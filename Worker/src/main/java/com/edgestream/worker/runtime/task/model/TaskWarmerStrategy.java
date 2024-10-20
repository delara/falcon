package com.edgestream.worker.runtime.task.model;

public enum TaskWarmerStrategy {
     NO_INITIAL_DEPLOYMENT //CLOUD deployment turns off all warm up features
    ,NO_STATEFUL_DUAL_ROUTED // An operator that uses a separate dual routing process that monitors tuple backlog
    ,NO_ZMQ_P2P // BROKER IS RESPONSIBLE  - used when a chain of operators is being warmed up and the source operator is getting its tuples from the broker
    ,YES_SAME_CONTAINER_TYPE_SWITCH_ZMQ // OPERATOR CONTAINER IS RESPONSIBLE  --consumer switch to different zmq port on the same container it warmed from after warmup to get live tuples AND the operator container must send a switch output type request to its predecessor
    ,YES_SAME_CONTAINER_TYPE_SWITCH_AMQ // OPERATOR CONTAINER IS RESPONSIBLE  --consumer switch from zmq to AMQ broker after the warmup to get live tuples
    ,YES_PREDECESSOR_CONTAINER // OPERATOR CONTAINER IS RESPONSIBLE --used when an operator must warm from a predecessor container AND we send it a switch to ZMQ message just in case its writing out to the broker, if it is alrady write to ZMQ it will ignore the request
    ,YES_ACTIVEMQ // BROKER IS RESPONSIBLE - used when the broker will control the warmup and send a stable tuple
}
