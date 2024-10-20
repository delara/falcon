package com.edgestream.clustermanager.model;

import com.edgestream.worker.runtime.plan.Physical_Plan;


public class Job {

    String JobID;
    String taskBrokerAddress;
    Physical_Plan pp;

    public Job(String jobID, String taskBrokerAddress, Physical_Plan pp) {
        JobID = jobID;
        this.taskBrokerAddress = taskBrokerAddress;
        this.pp = pp;
    }



}
