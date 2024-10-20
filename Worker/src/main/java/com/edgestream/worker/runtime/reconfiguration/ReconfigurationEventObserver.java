package com.edgestream.worker.runtime.reconfiguration;


import com.edgestream.worker.metrics.cloudDB.CloudMetricsDBReader;
import com.edgestream.worker.runtime.network.DataCenterManager;
import com.edgestream.worker.runtime.reconfiguration.data.*;
import com.edgestream.worker.runtime.reconfiguration.execution.ExecutionPlan;
import com.edgestream.worker.runtime.reconfiguration.triggers.model.ReconfigurationTrigger;
import org.apache.commons.lang3.tuple.ImmutablePair;
import java.util.ArrayList;
import java.util.HashMap;

/**************************************************************************************************************************
 * This class is a thread that is started by the Metrics Monitor. It constantly interrogates the Derby database for the latest stats and then
 * examines the rules, will trigger and action if a violation occurs
 **************************************************************************************************************************/
public class ReconfigurationEventObserver extends Thread {

    private final DataCenterManager dataCenterManager;
    private final CloudMetricsDBReader cloudMetricsDBReader;
    private final ArrayList<ReconfigurationTrigger> reconfigurationTriggers;
    private final ArrayList<Edge> edges = new ArrayList<>(); //the logical plan/dag, this never changes
    private final TaskDispatcher taskDispatcher;
    private final HashMap<String, HashMap<String,Integer>> operatorReplicaTracker = new HashMap<>(); //shared by all triggers

    public ReconfigurationEventObserver(DataCenterManager dataCenterManager, CloudMetricsDBReader cloudMetricsDBReader, ArrayList<ReconfigurationTrigger> reconfigurationTriggers, ExecutionPlan executionPlan, TaskDispatcher taskDispatcher) {
        this.dataCenterManager = dataCenterManager;
        this.cloudMetricsDBReader = cloudMetricsDBReader;
        this.reconfigurationTriggers = reconfigurationTriggers;
        this.taskDispatcher = taskDispatcher;

        registerTriggersWithReplicaTracker();

        //1. parse dag into a list of Edges
        for(ImmutablePair operatorToChildPair : executionPlan.getDag()){
            String[] edge = new String[2];
            edge[0] = String.valueOf(operatorToChildPair.left);
            edge[1] = String.valueOf(operatorToChildPair.right);
            edges.add(new Edge(edge));

        }

    }


    @Override
    public void run() {

        System.out.println("Starting Event observer");

        while(true){ // run forever
            System.out.println("##################Event Cycle BEGIN#######################");

            ArrayList<ComputingResource> computingResources = this.cloudMetricsDBReader.getAllComputingResourceStats(this.dataCenterManager.getRootDataCenter()); //get the hardware metrics
            ArrayList<NetworkLink> networkLinks = this.cloudMetricsDBReader.getAllNetworkLinkStats(this.dataCenterManager.getRootDataCenter());  //get the network metrics
            ArrayList<OperatorMap> operatorMaps = this.cloudMetricsDBReader.getAllOperatorMaps(this.dataCenterManager.getRootDataCenter());  //get the network metrics
            ArrayList<OperatorStatistics> operatorStatistics = this.cloudMetricsDBReader.getAllOperatorStatistics(operatorMaps);
//            ArrayList<OperatorPartitionKeyStats> operatorPartitionKeyStats = this.cloudMetricsDBReader.getAllOperatorPartitionKeyStats(operatorMaps);

            //1.Print all the stats
            System.out.println("------------DAG---------------------------------------");
            for(Edge edge: edges){
                System.out.println(edge.toTuple());
            }

            System.out.println("------------Compute Resource Stats---------------------------------------");

            for(ComputingResource computingResource: computingResources){
                if(computingResource!= null) {
                    System.out.println(computingResource.toTuple());
                }
            }

            System.out.println("-----------Network Stats-------------------------------------------");


            for(NetworkLink networkLink: networkLinks){
                if(networkLink!= null) {
                    System.out.println(networkLink.toTuple());
                }
            }


            System.out.println("-------------Operator Mapping-----------------------------------------");


            for(OperatorMap operatorMap: operatorMaps){
                if(operatorMap!= null) {
                    System.out.println(operatorMap.toTuple());
                }else{
                    System.out.println("No operator map");
                }
            }

            System.out.println("----------------Operator Statistics--------------------------------------");

            for(OperatorStatistics operatorStatistic: operatorStatistics){
                if(operatorStatistic!= null) {
                    System.out.println(operatorStatistic.toTuple());
                }else{
                    System.out.println("No operator stats");
                }
            }


//            System.out.println("----------------Operator State Statistics--------------------------------------");
//            for(OperatorPartitionKeyStats operatorPartitionKeyStat: operatorPartitionKeyStats){
//                if(operatorPartitionKeyStat!= null) {
//                    System.out.println(operatorPartitionKeyStat.toTuple());
//                }else{
//                    System.out.println("No operator partition key stats");
//                }
//            }




            //Add all the stats objects to the master stats object so that it can be passed as bundle to the triggers.
            try {
                ReconfigurationStats reconfigurationStats = new ReconfigurationStats(computingResources, edges, networkLinks, operatorMaps, operatorStatistics);
                checkTriggers(reconfigurationStats);
            }catch(Exception e){
                e.printStackTrace();
                System.out.println("Stats not complete, must wait for next reading");

            }


            try {
                Thread.sleep(100); //check every 5 seconds for new stats
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void registerTriggersWithReplicaTracker(){
        for(ReconfigurationTrigger reconfigurationTrigger: reconfigurationTriggers){

            reconfigurationTrigger.registerOperatorReplicaTracker(operatorReplicaTracker);
        }

    }



    private void checkTriggers(ReconfigurationStats reconfigurationStats){

        for(ReconfigurationTrigger reconfigurationTrigger: reconfigurationTriggers){

            if(!reconfigurationTrigger.isTriggerAlreadyFired() && reconfigurationTrigger.ruleTriggered(reconfigurationStats)){
                for(ExecutionPlan executionPlan : reconfigurationTrigger.getExecutionPlans(this.dataCenterManager)) {

                    TaskDispatcher taskDispatcher = new TaskDispatcher(executionPlan);
                    taskDispatcher.submitJob();
                }
            }

        }
    }



}
