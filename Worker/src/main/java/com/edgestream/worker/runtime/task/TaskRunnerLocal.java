package com.edgestream.worker.runtime.task;

import com.edgestream.worker.io.MessageConsumerClient;
import com.edgestream.worker.runtime.task.model.TaskRequestSetupOperator;


/**
 * This implementation of the {@link TaskRunner} is intended to be used by a com.edgestream.worker.runtime.task.processor
 * to create an operator instance inside THIS jvm. This will likely never be used but is here if needed for testing.
 */
public class TaskRunnerLocal extends  TaskRunner implements Runnable {

    public TaskRunnerLocal(TaskRequestSetupOperator taskRequestSetupOperator, MessageConsumerClient consumer){
        super(taskRequestSetupOperator,consumer);
    }

    @Override
    public void run() {


        /**What this method does****
         * 1. Get tuples from the queue
         * 2. Call the operator once per tuple
         * 3. Operator will emit something, call the producer client to take the message and put it into the queue
         * 4. Repeat 1,2,3 forever
         * */

        this.consumer.startTask();
    }


}
