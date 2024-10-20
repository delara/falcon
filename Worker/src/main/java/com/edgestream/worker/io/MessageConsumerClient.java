package com.edgestream.worker.io;

import com.edgestream.worker.runtime.task.model.Task;
import com.edgestream.worker.runtime.task.model.TaskWarmerStrategy;

public abstract class MessageConsumerClient {

     public Task boundTask;
     private TaskWarmerStrategy taskWarmerStrategy;

     public abstract void startTask();

     public abstract void setOperatorIsInWarmUpPhase(boolean inWarmUpPhase);

     public void setTaskWarmerStrategy(TaskWarmerStrategy taskWarmerStrategy) {
          this.taskWarmerStrategy = taskWarmerStrategy;
     }

     public TaskWarmerStrategy getTaskWarmerStrategy() {
          return taskWarmerStrategy;
     }
}
