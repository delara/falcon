package com.edgestream.worker.runtime.task;


import com.edgestream.worker.runtime.topology.TopologyAttributeAddressManager;
import org.apache.activemq.artemis.api.core.management.QueueControl;

public class TaskWarmer extends Thread{

    private final String taskWarmerID;
    private final String localAddressToMonitor;
    private final String localQueueToMonitor;
    private final TaskWarmerManager taskWarmerManager;
    private QueueControl queueController;
    private final long messageCountThreshold = 10l;
    private final String FQQN;
    private final TopologyAttributeAddressManager topologyAttributeAddressManager;
    private int timeTic = 0;
    private final String reconfigurationPlanID;
    private boolean isTriggered = false;

    public TaskWarmer(String taskWarmerID, String localQueueToMonitor, String localAddressToMonitor, TaskWarmerManager taskWarmerManager, TopologyAttributeAddressManager topologyAttributeAddressManager, String reconfigurationPlanID) {
        this.taskWarmerID = taskWarmerID;
        this.localAddressToMonitor = localAddressToMonitor;
        this.localQueueToMonitor = localQueueToMonitor;
        this.taskWarmerManager = taskWarmerManager;
        this.FQQN = this.getLocalAddressToMonitor() + "::" + this.getLocalQueueToMonitor();
        System.out.println("[TaskWarmer]: Task warmer monitor for address and queue: " + this.FQQN  +  "has been created");
        this.topologyAttributeAddressManager = topologyAttributeAddressManager;
        this.reconfigurationPlanID = reconfigurationPlanID;
    }

    public String getReconfigurationPlanID() {
        return reconfigurationPlanID;
    }

    TopologyAttributeAddressManager getTopologyAttributeAddressManager(){
        return topologyAttributeAddressManager;

    }

    public String getLocalAddressToMonitor() {
        return localAddressToMonitor;
    }

    public String getLocalQueueToMonitor() {
        return localQueueToMonitor;
    }


    private int getSecondsToWait(){
        return 30;
    }


    public boolean isTriggered() {
        return isTriggered;
    }

    @Override
    public void run() {
        isTriggered = true;
        System.out.println("[TaskWarmer]: Starting warmer manager for: [" + this.FQQN + "], for this many seconds: [" + getSecondsToWait() +"]");
        boolean warmingComplete = false;
        while(!warmingComplete){
            if (getTimeTic() > getSecondsToWait()){ //TODO: add queue backlog checker since warmup tuples could be still in the queue
                warmingComplete = true;
                System.out.println("[TaskWarmer]: Warm up finished......");
                taskWarmerManager.notifyWarmUpComplete(this);
            }else{
                try{
                    Thread.sleep(1000);
                    tick();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }

        }
    }



    private int getTimeTic() {
        return timeTic;
    }

    private void tick(){

        timeTic++;
        System.out.println("[TaskWarmer]: Current time tick: " + timeTic + " warm up will terminate at time tick: " + getSecondsToWait());
    }
}
