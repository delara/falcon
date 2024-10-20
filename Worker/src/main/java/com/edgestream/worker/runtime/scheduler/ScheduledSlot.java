package com.edgestream.worker.runtime.scheduler;

import java.io.Serializable;

public class ScheduledSlot implements Serializable {

    private final String taskManagerID; //aka the datacenter id
    private final String scheduledHostID;


    public ScheduledSlot(String taskManagerID,String scheduledHostID){
        this.taskManagerID= taskManagerID;
        this.scheduledHostID = scheduledHostID;
    }


    public String getTaskManagerID() {
        return taskManagerID;
    }

    public String getScheduledHostID() {
        return scheduledHostID;
    }

    @Override
    public String toString() {
        return "ScheduledSlot{" +
                "taskManagerID='" + taskManagerID + '\'' +
                ", scheduledHostID='" + scheduledHostID + '\'' +
                '}';
    }
}
