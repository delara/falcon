package com.edgestream.worker.runtime.topology;

import com.edgestream.worker.operator.OperatorID;
import com.edgestream.worker.runtime.reconfiguration.common.DagPlan;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.ArrayDeque;
import java.util.HashMap;


/**
 * The responsibility of this class is to create a queue of operators that belong to the same reconfiguration plan.
 * For example, if we have 3 operators in a reconfiguration plan then 3 operators would be enqueued here. The purpose of
 * managing them in this way is because we want to group the setup of all operators so that we can know when the setup is finished
 * so that the address that feeds the first operator in the chain can be activated so that tuples can start flowing. If we activated
 * the address before completing the setup of all operators then tuples will accumulate in an operator until its successor comes online.
 */
public class TopologyReconfigurationManager {

    private final HashMap<String, DagPlan> reconfigurationHistory = new HashMap<>();
    private final HashMap<String, ArrayDeque<String>> topologySetupQueue = new HashMap<>();
    private final HashMap<String, ArrayDeque<OperatorID>> topologyOperatorOnlineConfirmationQueue = new HashMap<>();
    private final Topology boundTopology;

    public TopologyReconfigurationManager(Topology topology) {

        this.boundTopology = topology;

    }

    public synchronized void addReconfigurationRequest(String reconfigurationPlanID, DagPlan dagPlan){

        if(!reconfigurationHistory.containsKey(reconfigurationPlanID)){
            System.out.println("Topology reconfiguration request: " + reconfigurationPlanID + " not found, will add...");

            //Keeps a history of all reconfiguration requests
            reconfigurationHistory.put(reconfigurationPlanID,dagPlan);

            // A working queue of operators that need to be installed, the ArrayDeque will eventually become empty
            topologySetupQueue.put(reconfigurationPlanID,getDagSetupQueue(dagPlan));

            // A working queue of operators that are awaiting confirmation that they are setup and online
            topologyOperatorOnlineConfirmationQueue.put(reconfigurationPlanID,dagPlan.getOperatorIDsInDagPlan());
        }

    }





    private synchronized ArrayDeque getDagSetupQueue(DagPlan dagPlan){

        ArrayDeque<String> operatorsToBeInstalled = new ArrayDeque<>();
        for(ImmutablePair dagPlanElement: dagPlan.getDagElements()){

            operatorsToBeInstalled.add((String) dagPlanElement.left);

        }

        return operatorsToBeInstalled;
    }

    public synchronized void dequeOperatorFromSetupQueue(String reconfigurationPlanID, String operatorInputType){
        topologySetupQueue.get(reconfigurationPlanID).remove(operatorInputType);
        System.out.println(operatorInputType + " operator removed from setup queue");

    }


    public synchronized boolean isOperatorSetupQueueEmpty(String reconfigurationPlanID){
        System.out.println("Operator setup queue is empty? " +  topologySetupQueue.get(reconfigurationPlanID).isEmpty());
        return topologySetupQueue.get(reconfigurationPlanID).isEmpty();

    }


    /*****************************************************************************
     *
     *                  Operator Online Confirmation Features
     *
     **************************************************************************** */

    public synchronized void dequeOperatorFromConfirmationQueue(String reconfigurationPlanID, String operatorID){

       if(!isOperatorConfirmationQueueEmpty(reconfigurationPlanID)) {
           ArrayDeque<OperatorID> operatorIDS = topologyOperatorOnlineConfirmationQueue.get(reconfigurationPlanID);
           boolean operatorFound = false;
           for (OperatorID o : operatorIDS) {
               if (o.getOperatorID_as_String().equalsIgnoreCase(operatorID)) {
                   topologyOperatorOnlineConfirmationQueue.get(reconfigurationPlanID).remove(o);
                   System.out.println("Operator: " + operatorID + " has reported a status of [completed] " +
                           "and has now been removed from confirmation queue.");
                   operatorFound = true;
                   isOperatorConfirmationQueueEmpty(reconfigurationPlanID);
               }
           }

           if (!operatorFound) {
               System.out.println("Online Operator not found in DagPlan, something went wrong, investigate");
           }
       }
    }

    public synchronized boolean isOperatorConfirmationQueueEmpty(String reconfigurationPlanID){
        System.out.println("Operator confirmation queue is empty? " +  topologyOperatorOnlineConfirmationQueue.get(reconfigurationPlanID).isEmpty());
        return topologyOperatorOnlineConfirmationQueue.get(reconfigurationPlanID).isEmpty();
    }

}
