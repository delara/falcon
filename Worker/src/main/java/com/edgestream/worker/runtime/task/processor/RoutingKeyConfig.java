package com.edgestream.worker.runtime.task.processor;

import com.edgestream.worker.runtime.reconfiguration.state.AtomicKey;
import com.edgestream.worker.runtime.reconfiguration.state.RoutingKey;
import com.edgestream.worker.runtime.task.model.TaskRequestSetupOperator;

import java.util.ArrayList;

public class RoutingKeyConfig {


    private final TaskRequestSetupOperator taskRequestSetupOperator;
    private RoutingKey routingKey;



    protected RoutingKeyConfig(TaskRequestSetupOperator taskRequestSetupOperator) {
        this.taskRequestSetupOperator = taskRequestSetupOperator;

        //Always create an [Any] key. this will be replaced if there is an actual routing key
        ArrayList<AtomicKey> keyList = new ArrayList<>();
        keyList.add(new AtomicKey("any","any"));
        routingKey = new RoutingKey(keyList);

        configureRoutingKey();
    }


    protected RoutingKey getRoutingKey() {
        return routingKey;
    }

    private void configureRoutingKey(){
        //replace with actual key if it exists
        if (taskRequestSetupOperator.hasBootstrapState()) {
            routingKey = (RoutingKey) taskRequestSetupOperator.getStateTransferPlan().getOperatorStateObject(routingKey.getClass().getName());
        }

    }

}
