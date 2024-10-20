package com.edgestream.worker.runtime.task;

import com.edgestream.worker.io.MessageConsumerClient;
import com.edgestream.worker.runtime.docker.DockerFile;
import com.edgestream.worker.runtime.docker.DockerHost;
import com.edgestream.worker.runtime.task.model.TaskRequestSetupOperator;

public class TaskRunner {

    MessageConsumerClient consumer;
    TaskRequestSetupOperator taskRequestSetupOperator;


    public TaskRunner(TaskRequestSetupOperator taskRequestSetupOperator, MessageConsumerClient consumer){

        this.taskRequestSetupOperator = taskRequestSetupOperator;
        this.consumer = consumer;  // the consumer is responsible for invoking the Task because it is async

    }

    public TaskRequestSetupOperator getTaskRequestSetupOperator() {
        return taskRequestSetupOperator;
    }






}
