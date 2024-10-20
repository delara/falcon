package com.edgestream.worker.runtime.reconfiguration.execution;


import com.edgestream.worker.runtime.docker.DockerFileType;
import com.edgestream.worker.runtime.plan.Host;
import com.edgestream.worker.runtime.reconfiguration.common.DagPlan;
import com.edgestream.worker.runtime.task.model.TaskType;

import java.util.List;

public class ExecutionPlanConfig {

    private final String reconfigurationPlanID;
    private final String topologyID;
    private final List<Host> destinationDataCenterTaskManagers;
    private final DagPlan dagPlan;
    private int replicaID;
    private final DockerFileType dockerFileType;
    private final TaskType taskType;
    private final ExecutionType executionType;

    public ExecutionPlanConfig(String reconfigurationPlanID, String topologyID, List<Host> destinationDataCenterTaskManagers
            , DagPlan dagPlan, DockerFileType dockerFileType, TaskType taskType, ExecutionType executionType) {
        this.reconfigurationPlanID = reconfigurationPlanID;
        this.topologyID = topologyID;
        this.destinationDataCenterTaskManagers = destinationDataCenterTaskManagers;
        this.dagPlan = dagPlan;
        this.dockerFileType = dockerFileType;
        this.taskType = taskType;
        this.executionType = executionType;
    }


    public ExecutionType getExecutionType() {
        return executionType;
    }

    public String getReconfigurationPlanID() {
        return reconfigurationPlanID;
    }

    public String getTopologyID() {
        return topologyID;
    }

    public List<Host> getDestinationDataCenterTaskManagers() {
        return destinationDataCenterTaskManagers;
    }

    public DagPlan getDagPlan() {
        return dagPlan;
    }

    public DockerFileType getDockerFileType() {
        return dockerFileType;
    }

    public TaskType getTaskType() {
        return taskType;
    }

}
