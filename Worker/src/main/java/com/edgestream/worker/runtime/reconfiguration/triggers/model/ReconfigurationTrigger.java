package com.edgestream.worker.runtime.reconfiguration.triggers.model;

import com.edgestream.worker.runtime.docker.DockerFileType;
import com.edgestream.worker.runtime.network.DataCenter;
import com.edgestream.worker.runtime.network.DataCenterManager;
import com.edgestream.worker.runtime.network.RootDataCenter;
import com.edgestream.worker.runtime.plan.Host;
import com.edgestream.worker.runtime.reconfiguration.common.DagPlan;
import com.edgestream.worker.runtime.reconfiguration.execution.ExecutionPlan;
import com.edgestream.worker.runtime.reconfiguration.data.ReconfigurationStats;
import com.edgestream.worker.runtime.reconfiguration.execution.ExecutionPlanConfig;
import com.edgestream.worker.runtime.reconfiguration.execution.ExecutionType;
import com.edgestream.worker.runtime.task.model.TaskType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public abstract class ReconfigurationTrigger {


    private final String triggerID;
    private int timeTic = 0;


    private final String topologyID;
    private boolean triggerAlreadyFired = false;
    private HashMap<String, HashMap<String,Integer>> operatorReplicaTracker;
    private int firedCounter = 0;

    public ReconfigurationTrigger(String triggerID, String topologyID) {
        this.triggerID = triggerID;
        this.topologyID = topologyID;
    }

    public String getTriggerID() {
        return triggerID;
    }

    public String getTopologyID() {
        return topologyID;
    }

    public int getFiredCounter() {
        return firedCounter;
    }


    public void printTriggerState(boolean triggerState){

        System.out.println("Reconfiguration rule "+ getTriggerID() + " triggered?: " + triggerState);
        tick();
    }



    public boolean isTriggerAlreadyFired() {
        return triggerAlreadyFired;
    }

    public void fireTrigger(){
        triggerAlreadyFired = true;
        firedCounter++;
    }
    public void registerOperatorReplicaTracker(HashMap<String, HashMap<String,Integer>> operatorReplicaTracker){

        this.operatorReplicaTracker = operatorReplicaTracker;

    }

    protected int timeToTriggerMinutes(int timeMinutes){

        int triggerTime = (timeMinutes*60) / 5;
        System.out.println("Trigger time tick: " + triggerTime);
        return   triggerTime;
    }

    protected int getTimeTic() {
        return timeTic;
    }

    private void tick(){

        timeTic++;
        System.out.println("Current time tick: " + timeTic);
    }




    public int addOperatorReplicaToTracker(String datacenterID,String operatorType){
        int replicaID = 0;
        //check to see if this data center even exists in the tracker
        if(!operatorReplicaTracker.containsKey(datacenterID)){
            //1. add the data center
            int initialID= 2;
            HashMap<String,Integer> operatorAndReplicaCounter = new HashMap<>();
            operatorAndReplicaCounter.put(operatorType,initialID);// replicas are always created after the first deployment which always uses replicaID = 1
            //2. add the data center and the replica counter
            operatorReplicaTracker.put(datacenterID,operatorAndReplicaCounter);
            replicaID = initialID;
        }
        //the datacenter already exists so just check to see if the operator counter exists if not create it first before adding the replica
        else{
            if(!operatorReplicaTracker.get(datacenterID).containsKey(operatorType)){
                int initialID= 2;
                operatorReplicaTracker.get(datacenterID).put(operatorType,initialID);
                replicaID = initialID;
            }else{
                int currentReplicaCount = operatorReplicaTracker.get(datacenterID).get(operatorType);
                currentReplicaCount++;
                operatorReplicaTracker.get(datacenterID).replace(operatorType,currentReplicaCount);
                replicaID = currentReplicaCount;
            }


        }

        return replicaID;

    }


    public abstract boolean ruleTriggered(ReconfigurationStats reconfigurationStats);

    public abstract ArrayList<ExecutionPlan> getExecutionPlans(DataCenterManager dataCenterManager);


    protected ExecutionPlan buildExecutionPlan(DataCenterManager dataCenterManager, ExecutionPlan executionPlan, TaskType taskType, String targetTaskManagerDeployment, DagPlan dagPlan,DockerFileType dockerFileType){

        String reconfigurationPlanID = taskType + "_" + this.getClass().getSimpleName()  + "_" + getFiredCounter();
        RootDataCenter rootDataCenter = dataCenterManager.getRootDataCenter();
        DataCenter targetDataCenter = rootDataCenter.searchForDataCenterByTaskManagerID(rootDataCenter,targetTaskManagerDeployment);
        List<Host> destinationDataCenterTaskManagers = targetDataCenter.getManageHostsList();
        ExecutionPlanConfig executionPlanConfig = new ExecutionPlanConfig(reconfigurationPlanID,topologyID,destinationDataCenterTaskManagers,dagPlan,dockerFileType, taskType, ExecutionType.RECONFIGURATION); // change to INITIAL to disable warm up
        executionPlan.createReconfigurationPlan(executionPlanConfig);

        return executionPlan;

    }

    protected void sleepForSeconds(int secondsToSleep){

        System.out.println("Trigger "+ getTriggerID() + " "+ "fired, sleeping for "+ secondsToSleep +" second(s)....");

        try {
            Thread.sleep(1000 * secondsToSleep);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


}
