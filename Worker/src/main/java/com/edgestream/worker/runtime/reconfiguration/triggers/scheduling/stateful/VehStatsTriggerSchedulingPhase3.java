package com.edgestream.worker.runtime.reconfiguration.triggers.scheduling.stateful;



import com.edgestream.worker.runtime.docker.DockerFileType;
import com.edgestream.worker.runtime.network.DataCenterManager;
import com.edgestream.worker.runtime.reconfiguration.common.DagPlan;
import com.edgestream.worker.runtime.reconfiguration.data.ReconfigurationStats;
import com.edgestream.worker.runtime.reconfiguration.execution.ExecutionPlan;
import com.edgestream.worker.runtime.reconfiguration.execution.VehichleStatsExecutionPlan;
import com.edgestream.worker.runtime.reconfiguration.state.AtomicKey;
import com.edgestream.worker.runtime.reconfiguration.state.RoutingKey;
import com.edgestream.worker.runtime.reconfiguration.state.StateTransferPlan;
import com.edgestream.worker.runtime.reconfiguration.triggers.model.ReconfigurationTrigger;
import com.edgestream.worker.runtime.scheduler.ScheduledSlot;
import com.edgestream.worker.runtime.task.model.TaskType;

import java.util.ArrayList;

public class VehStatsTriggerSchedulingPhase3 extends ReconfigurationTrigger {

    public VehStatsTriggerSchedulingPhase3(String triggerID, String topologyID) {

        super(triggerID,topologyID);

    }



    @Override
    public boolean ruleTriggered(ReconfigurationStats reconfigurationStats) {
        boolean localRuleTriggered = false;

//        for (OperatorPartitionKeyStats operatorPartitionKeyStats : reconfigurationStats.getOperatorPartitionKeyStats()) {
//
//            if (operatorPartitionKeyStats != null && !localRuleTriggered) {
//
//                if (operatorPartitionKeyStats.getTaskManagerID().equalsIgnoreCase("W001")
//                        && operatorPartitionKeyStats.getOperatorID().equalsIgnoreCase("OP_R_001")
//                        && operatorPartitionKeyStats.getStateCollectionObject().getKey("ci4wg4xti000502tccs34dvk421humidity").getCounter() > 30
//                        ) {
//
//                    sleepForMinutes(1); //for testing purposes
//                    localRuleTriggered = true;
//                }
//            }
//        }


        sleepForSeconds(90); //for testing purposes
        localRuleTriggered = true;

        System.out.println("Reconfiguration rule "+ this.getTriggerID() + " triggered?: " + localRuleTriggered);
        printTriggerState(localRuleTriggered);
        return localRuleTriggered;
    }

    @Override
    public ArrayList<ExecutionPlan> getExecutionPlans(DataCenterManager dataCenterManager) {
        fireTrigger();

        TaskType taskType = TaskType.CREATE;
        ArrayList<ExecutionPlan> executionPlans = new ArrayList<>();

        /***FOR W003***/

        DagPlan dagPlan_W003 = new DagPlan();
        String targetTaskManagerDeployment_W003 = "W003"; //create a replica at W003;
        ExecutionPlan executionPlan_W003 = new VehichleStatsExecutionPlan();

        /****
         * Add StateTransferPlan
         **/

        StateTransferPlan stateTransferPlan_W003 = new StateTransferPlan();

        ArrayList<AtomicKey> keyList = new ArrayList<>();

        // Keys for Trisk and Flink expt
//        AtomicKey sensorID1Key = new AtomicKey("vehicle_id","B");
//        AtomicKey sensorID2Key = new AtomicKey("vehicle_id","C");
//        AtomicKey sensorID3Key = new AtomicKey("vehicle_id","E");
//        AtomicKey sensorID4Key = new AtomicKey("vehicle_id","H");
//        AtomicKey sensorID5Key = new AtomicKey("vehicle_id","I");
//        AtomicKey sensorID6Key = new AtomicKey("vehicle_id","J");
//        keyList.add(sensorID1Key);
//        keyList.add(sensorID2Key);
//        keyList.add(sensorID3Key);
//        keyList.add(sensorID4Key);
//        keyList.add(sensorID5Key);
//        keyList.add(sensorID6Key);

        //Keys for state size expt
//        AtomicKey sensorID1Key = new AtomicKey("vehicle_id","A");
//        keyList.add(sensorID1Key);

        //Keys for multiple keys expt
        for (int i=200;i<300;i++) {
            String key = "A" + i;
            AtomicKey sensorID1Key = new AtomicKey("vehicle_id",key);
            keyList.add(sensorID1Key);
        }

        RoutingKey routingKey_W003 = new RoutingKey(keyList);

        routingKey_W003.activateSynchronization(getTopologyID(),"V","W003", routingKey_W003);


        stateTransferPlan_W003.addToTransferPayload(routingKey_W003.getClass().getName(), routingKey_W003);
        ScheduledSlot scheduledSlot_W003 = new ScheduledSlot(targetTaskManagerDeployment_W003,"0");


        dagPlan_W003.addStatefulDagPlanElement("V",addOperatorReplicaToTracker(targetTaskManagerDeployment_W003,"V"),stateTransferPlan_W003,scheduledSlot_W003);
        executionPlans.add(buildExecutionPlan(dataCenterManager, executionPlan_W003, taskType, targetTaskManagerDeployment_W003, dagPlan_W003, DockerFileType.EDGESTREAM_STATEFUL));

        return executionPlans;
    }
}
