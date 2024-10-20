package com.edgestream.worker.runtime.reconfiguration.state;



import java.io.Serializable;
import java.util.HashMap;

public class StateTransferPlan implements Serializable {

    private final HashMap<String,Object> stateObjectsToTransfer = new HashMap<>();


    /**
     *
     * @param key  This is the object type ex object.getClass().getName()
     * @param objectToTransfer
     */
    public void addToTransferPayload(String key, Object objectToTransfer){

        stateObjectsToTransfer.put(key,objectToTransfer);
    }

    public Object getOperatorStateObject(String key){

        return stateObjectsToTransfer.get(key);
    }

    public boolean stateTransferPlanContainsKey(String key){

        return stateObjectsToTransfer.containsKey(key);

    }


}
