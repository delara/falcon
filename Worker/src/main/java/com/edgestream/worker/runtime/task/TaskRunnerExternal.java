package com.edgestream.worker.runtime.task;

import com.edgestream.worker.io.MessageConsumerClient;
import com.edgestream.worker.runtime.docker.DockerFile;
import com.edgestream.worker.runtime.docker.DockerHost;
import com.edgestream.worker.runtime.docker.DockerHostID;
import com.edgestream.worker.runtime.docker.DockerHostManager;
import com.edgestream.worker.runtime.task.model.TaskRequestSetupOperator;


/**
 * This implementation of the {@link TaskRunner} is intended to be used by the {@link com.edgestream.worker.runtime.task.processor.TaskRequestProcessorContainer}
 * to setup an operator that will run in a Docker Container on a machine that can host docker containers.
 *
 */
public class TaskRunnerExternal extends  TaskRunner {

    private DockerHostID dockerHostID;
    private DockerFile dockerFile;

    public TaskRunnerExternal(TaskRequestSetupOperator taskRequestSetupOperator, MessageConsumerClient consumer){
        super(taskRequestSetupOperator,consumer);
    }

    public void addDockerFile(DockerFile dockerFile){
        this.dockerFile = dockerFile;
    }

    public DockerHostID getDockerHostID() {
        return dockerHostID;
    }

    public DockerFile getDockerFile() {
        return dockerFile;
    }

    public void setDockerHostID(DockerHostID dockerHostID) {
        this.dockerHostID = dockerHostID;
    }


}
