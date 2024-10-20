package com.edgestream.worker.runtime.task;


import com.edgestream.worker.runtime.topology.TopologyAttributeAddressManager;
import org.apache.activemq.artemis.api.core.management.QueueControl;

public class TaskMigration implements Runnable{

    private final String taskMigrationID;
    private final String localAddressToMonitor;
    private final String localQueueToMonitor;
    private final TaskMigrationManager taskMigrationManager;
    private QueueControl queueController;
    private long messageCountThreshold = 10l;
    private final String FQQN;
    private final TopologyAttributeAddressManager topologyAttributeAddressManager;


    public TaskMigration(String taskMigrationID, String localQueueToMonitor, String localAddressToMonitor, TaskMigrationManager taskMigrationManager, TopologyAttributeAddressManager topologyAttributeAddressManager) {
        this.taskMigrationID = taskMigrationID;
        this.localAddressToMonitor = localAddressToMonitor;
        this.localQueueToMonitor = localQueueToMonitor;
        this.taskMigrationManager = taskMigrationManager;
        this.FQQN = this.getLocalAddressToMonitor() + "::" + this.getLocalQueueToMonitor();
        System.out.println("Migration monitor for address and queue: " + this.FQQN  +  "has been created");
        this.topologyAttributeAddressManager = topologyAttributeAddressManager;
    }


    public void setMessageCountThreshold(long messageCountThreshold) {
        this.messageCountThreshold = messageCountThreshold;
    }

    TopologyAttributeAddressManager getTopologyAttributeAddressManager(){
        return topologyAttributeAddressManager;

    }


    public void setQueueController(QueueControl queueController){
        this.queueController = queueController;

    }

    long getTupleInQueueCount(){
        long messageCount = this.queueController.getMessageCount();

//         System.out.println("Messages in queue:  " + this.getLocalAddressToMonitor() + "::" + this.getLocalQueueToMonitor() +":" + messageCount );
        return messageCount;
    }


    public String getLocalAddressToMonitor() {
        return localAddressToMonitor;
    }

    public String getLocalQueueToMonitor() {
        return localQueueToMonitor;
    }


    @Override
    public void run() {

        System.out.println("Starting migration manager for: " + this.FQQN);
        boolean migrationComplete = false;
        try{
            Thread.sleep(1);
        }catch (Exception e){
            e.printStackTrace();
        }
        while(!migrationComplete){

            long messageCount = getTupleInQueueCount();
            
            // For debugging purpose, forcefully setting messageCount to 1.
            messageCount = 1;

            if(messageCount < this.messageCountThreshold && messageCount >= 1){
                System.out.println("Migration complete, activating queues....");
                migrationComplete = true;
                System.out.println("Notifying the manager the migration is complete...");
                this.taskMigrationManager.notifyMigrationComplete(this);
            }else{
//                 System.out.println("There are more than 100 messages in " + this.FQQN);
                try{
                    Thread.sleep(10);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        //Using a fixed timer instead  of checking messages in queue
        //This is for deciding when to release the stable tuple marker
//        int sleepTimeInMilliSeconds = 5000;
//        System.out.println("Waiting for " + sleepTimeInMilliSeconds + " ms before releasing stable tuple marker");
//        try{
//            Thread.sleep(sleepTimeInMilliSeconds);
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        System.out.println("Migration complete, activating queues....");
//        System.out.println("Notifying the manager the migration is complete...");
//        this.taskMigrationManager.notifyMigrationComplete(this);
    }
}
