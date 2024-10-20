package com.edgestream.worker.runtime.task;

import com.edgestream.worker.runtime.task.broker.TaskBrokerConfig;
import com.edgestream.worker.runtime.topology.Topology;


import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.HashMap;

public class TaskWarmerManager {

    HashMap<String,TaskWarmer> warmingTasks = new HashMap<>();
    ArrayList<TaskWarmer> completedWarmingTasks = new ArrayList<>();
    TaskBrokerConfig taskBrokerConfig;
    Topology boundTopology;

    public TaskWarmerManager(Topology topology) {

        this.boundTopology = topology;
        this.taskBrokerConfig = topology.getBoundTaskDriver().getTaskBrokerConfig();
    }


    public void addFutureWarmUpTask(TaskWarmer taskWarmer){
        warmingTasks.put(taskWarmer.getReconfigurationPlanID(),taskWarmer);
    }


    public void triggerWarmUp(String reconfigurationPlanID){
        System.out.println("[TaskWarmerManager]: Checking to see if I need to trigger the warmup");

        if (warmingTasks.containsKey(reconfigurationPlanID)) {
            TaskWarmer taskWarmerToStart = warmingTasks.get(reconfigurationPlanID);
            if (!taskWarmerToStart.isTriggered()) {
                System.out.println("[TaskWarmerManager]: Triggering warmer....");
                taskWarmerToStart.start();
            } else {
                System.out.println("[TaskWarmerManager]: Do not need to trigger warmer");
            }
        }else{
            System.out.println("[TaskWarmerManager]: No warming task exists for this operator");
        }

    }


    /**
     * This is later called by the TaskWarmer that was started by the method triggerWarmUp(String reconfigurationPlanID)
     * @param taskWarmer
     */
    public void notifyWarmUpComplete(TaskWarmer taskWarmer){
        taskWarmer.getTopologyAttributeAddressManager().uninstallDualRouteToTopologyNoMatch();
        warmingTasks.remove(taskWarmer.getReconfigurationPlanID());
        completedWarmingTasks.add(taskWarmer);

    }

}
