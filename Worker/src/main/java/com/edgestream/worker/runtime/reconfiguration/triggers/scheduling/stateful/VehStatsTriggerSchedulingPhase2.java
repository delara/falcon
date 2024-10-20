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


/***
 * This phase requires that operator P and R be placed on core1. Operator R will be started with a specific
 * partition key at boot time. For now, this key will be hardcoded into this trigger. The counter threshold will be hardcoded also.
 * The logic for this trigger is as follows:
 *      if the operator is in datacenter W001
 *          if the operator id is OP_R_001
 *              if the key_value = ci4wg4xti000502tccs34dvk421humidity
 *                  if the key_counter > 30
 *                      create operator P on W002
 *                      create operator R on W002 with key_value = ci4wg4xti000502tccs34dvk421humidity
 *
 */

public class VehStatsTriggerSchedulingPhase2 extends ReconfigurationTrigger {

    public VehStatsTriggerSchedulingPhase2(String triggerID, String topologyID) {

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


        sleepForSeconds(60); //for testing purposes
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
        for(int i=1;i<=100;i++) {
            for(int j = 0; j<4; j++) {
                String keyId = "A" + (i + j*400);
                AtomicKey sensorID1Key = new AtomicKey("vehicle_id",keyId);
                keyList.add(sensorID1Key);
            }
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
