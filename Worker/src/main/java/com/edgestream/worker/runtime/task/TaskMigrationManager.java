package com.edgestream.worker.runtime.task;

import com.edgestream.worker.runtime.task.broker.TaskBrokerConfig;
import com.edgestream.worker.runtime.topology.Topology;
import org.apache.activemq.artemis.api.core.management.QueueControl;

import java.util.ArrayDeque;
import java.util.ArrayList;

public class TaskMigrationManager {

    ArrayDeque<TaskMigration> migratingTasks = new ArrayDeque<>();
    ArrayList<TaskMigration> completedMigratedTasks = new ArrayList<>();
    TaskBrokerConfig taskBrokerConfig;
    Topology boundTopology;

    public TaskMigrationManager(Topology topology) {

        this.boundTopology = topology;
        this.taskBrokerConfig = topology.getBoundTaskDriver().getTaskBrokerConfig();
    }


    public void addAndTriggerMigration(TaskMigration taskMigration){

        taskMigration.setQueueController(getQueueController(taskMigration.getLocalAddressToMonitor(),taskMigration.getLocalQueueToMonitor()));
        taskMigration.run();
        migratingTasks.add(taskMigration);


    }


    private QueueControl getQueueController(String address, String queueName){


        return taskBrokerConfig.getQueueController(address,queueName);

    }

    public void notifyMigrationComplete(TaskMigration taskMigration){

        taskMigration.getTopologyAttributeAddressManager().shutDownDualRouting();
        migratingTasks.remove(taskMigration);
        completedMigratedTasks.add(taskMigration);



    }

}
