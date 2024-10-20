package com.edgestream.worker.runtime.reconfiguration;

import com.edgestream.clustermanager.manager.MainClusterManagerAPI;
import com.edgestream.worker.config.EdgeStreamGetPropertyValues;
import com.edgestream.worker.runtime.reconfiguration.execution.ExecutionPlan;
import com.edgestream.worker.runtime.reconfiguration.execution.TwitterExecutionPlan;


public class TaskDispatcher {

    MainClusterManagerAPI mainClusterManagerAPI;
    ExecutionPlan executionPlan;
    String taskBrokerAddress;

    public TaskDispatcher(ExecutionPlan executionPlan) {
        this.mainClusterManagerAPI = new MainClusterManagerAPI();
        this.executionPlan = executionPlan;
        this.taskBrokerAddress = "tcp://"+ EdgeStreamGetPropertyValues.getCLUSTER_MANAGER_IP() +":" + EdgeStreamGetPropertyValues.getCLUSTER_MANAGER_PORT(); // TODO: For now this is the cloud root, later this could be the local broker in this datacenter
    }

    public void submitJob(){
        this.mainClusterManagerAPI.submitJob(taskBrokerAddress,executionPlan);
    }

    /**
     * Use this if you want to run in stand alone mode
     * */
    public static void main(String[] args) {
      new TaskDispatcher(new TwitterExecutionPlan()).submitJob();
      //new TaskDispatcher(new ETLExecutionPlan()).submitJob();
    }
}
