package com.edgestream.clustermanager.main;

import com.edgestream.worker.config.EdgeStreamGetPropertyValues;
import com.edgestream.worker.metrics.cloudDB.CloudMetricsMessageParser;
import com.edgestream.worker.metrics.model.Metric;
import com.edgestream.worker.runtime.docker.DockerFileType;
import com.edgestream.worker.runtime.network.DataCenterManager;
import com.edgestream.worker.runtime.network.Network;
import com.edgestream.worker.runtime.network.RootDataCenter;
import com.edgestream.worker.runtime.plan.Host;
import com.edgestream.worker.runtime.reconfiguration.ReconfigurationEventObserver;
import com.edgestream.worker.runtime.reconfiguration.TaskDispatcher;
import com.edgestream.worker.runtime.reconfiguration.common.DagPlan;
import com.edgestream.worker.runtime.reconfiguration.execution.*;
import com.edgestream.worker.runtime.reconfiguration.triggers.model.ReconfigurationTrigger;
import com.edgestream.worker.runtime.reconfiguration.triggers.scheduling.stateful.VehStatsTriggerSchedulingPhase1;

import com.edgestream.worker.runtime.scheduler.ScheduledSlot;
import com.edgestream.worker.runtime.task.model.TaskType;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQUnBlockedException;
import org.apache.activemq.artemis.api.core.client.*;


import java.util.ArrayList;
import java.util.List;


public class FalconClientManager {

    ClientConsumer consumer;
    ClientSession session;
    DataCenterManager  dataCenterManager;
    CloudMetricsMessageParser cloudMetricsMessageParser;
    ReconfigurationEventObserver reconfigurationEventObserver;
    int rounds = 0;
    int commitFrequency = 50;

    public static void main(final String[] args) throws Exception {
        new FalconClientManager("metrics_consumer_cloud_001" , EdgeStreamGetPropertyValues.getCLOUD_ROOT_DATACENTER_IP()+ ":61616");
    }



    public FalconClientManager(String monitorName, String broker_ip) throws Exception {

        System.out.println("Starting metrics monitor: " + monitorName);
        ServerLocator locator = ActiveMQClient.createServerLocator("tcp://" + broker_ip);
        locator.setConfirmationWindowSize(3000000);
        locator.setBlockOnDurableSend(false);
        locator.setBlockOnNonDurableSend(false);
        locator.setBlockOnAcknowledge(false);
        ClientSessionFactory factory =  locator.createSessionFactory();
        session = factory.createTransactedSession();
        String queueAddress  = "primary_merlin_metrics_address::primary_metrics_queue";


        try {
            consumer = session.createConsumer(queueAddress, false);

        } catch (ActiveMQException e) {
            e.printStackTrace();
        }

        /***Setup a specific network topology TODO: not yet dynamic, this needs to be set before each experiment************/

        Network network = new Network();
        RootDataCenter rootDataCenter = null;

        if(EdgeStreamGetPropertyValues.getNETWORK_LAYOUT().equals("aws2t")) {
            rootDataCenter = network.getAWS2TierNetwork();
        }

        //we only need to the root because we can traverse the tree to find other data centers
        this.dataCenterManager = new DataCenterManager(rootDataCenter);
        this.cloudMetricsMessageParser = new CloudMetricsMessageParser(this.dataCenterManager);



        /************************************************************************************************************
         *
         * Application & Reconfiguration Setup
         *
         *
         * TODO:The application & reconfiguration setup needs to be moved to be part of a standalone process that accepts jobs and manages them
         *  so we can handle multiple jobs for now we have to manually create them using the TaskDispatcher and invoke them here
         *  inside the Metrics monitor
         *
         ************************************************************************************************************/

        System.out.println("Upload started: " + System.currentTimeMillis());
        VehStatsApplicationSetup(reconfigurationEventObserver, dataCenterManager, rootDataCenter, cloudMetricsMessageParser);

        System.out.println("Upload completed: " + System.currentTimeMillis());

        /****************************************************************************************************
         *
         *
         * Reconfiguration Thread configuration END
         *
         *
         *
         * *************************************************************************************************/


        System.out.println("Starting ActiveMQ metrics message consumer");
        consumer.setMessageHandler(new MetricsMessageHandler(cloudMetricsMessageParser));
        session.start();
        Thread.sleep(7000000);
        session.close();

        System.out.println("Metrics Monitor: " + monitorName +  "has been terminated");
    }

    class MetricsMessageHandler implements MessageHandler  {

        CloudMetricsMessageParser cloudMetricsMessageParser;


        public MetricsMessageHandler(CloudMetricsMessageParser cloudMetricsMessageParser) {

            this.cloudMetricsMessageParser = cloudMetricsMessageParser;


        }

        public void retry(ClientMessage clientMessage) {
            Metric metric = cloudMetricsMessageParser.parseMetricMessage(clientMessage);

            //System.out.println(metric.toTuple());

            try {
                session.start();
                clientMessage.acknowledge();
                session.commit();
            } catch (ActiveMQException e) {
                e.printStackTrace();
            }
        }


        @Override
        public void onMessage(ClientMessage clientMessage) {

            Metric metric = cloudMetricsMessageParser.parseMetricMessage(clientMessage);

            try {
                clientMessage.acknowledge();
                if (rounds % commitFrequency == 0) {
                    session.commit();
                }
                rounds++;
            } catch (ActiveMQUnBlockedException acmq) {
                System.out.println("Error occurred ... retrying");
                retry(clientMessage);
            } catch (ActiveMQException e) {
                e.printStackTrace();
            }

        }
    }

    public static void VehStatsApplicationSetup(ReconfigurationEventObserver reconfigurationEventObserver, DataCenterManager dataCenterManager, RootDataCenter rootDataCenter, CloudMetricsMessageParser cloudMetricsMessageParser) {
        /* VEHICLE STATS APPLICATION SETUP*/
        ExecutionPlan executionPlan = new VehichleStatsExecutionPlan();


        String reconfigurationPlanID = "VEH_STATS_001_INITIAL";
        String topologyID = "vehstats001";
        cloudMetricsMessageParser.writeToEdgeStreamLog("----EXP1-Cloud-Edge-VEHSTATS---");

        ScheduledSlot scheduledSlot = new ScheduledSlot("W001","0");
        DagPlan dagPlan = new DagPlan();

        int replicaID = 1; //always one because this the start

        dagPlan.addDagPlanElement("V",replicaID,scheduledSlot);
        dagPlan.addDagPlanElement("D",replicaID,scheduledSlot);

        List<Host> destinationDataCenterTaskManagers = rootDataCenter.getManageHostsList(); // we always deploy to the root data center first
        ExecutionPlanConfig executionPlanConfig = new ExecutionPlanConfig(reconfigurationPlanID,topologyID,destinationDataCenterTaskManagers,dagPlan, DockerFileType.EDGESTREAM_STATEFUL, TaskType.CREATE, ExecutionType.INITIAL);
        executionPlan.createReconfigurationPlan(executionPlanConfig);

        TaskDispatcher taskDispatcher = new TaskDispatcher(executionPlan);
        taskDispatcher.submitJob(); // This sends the execution plan to the ClusterManager and it will be deployed immediately

        //Reconfiguration Thread setup and initialisation
        ArrayList<ReconfigurationTrigger> reconfigurationTriggers = new ArrayList<>();
        ReconfigurationTrigger VehStatsTriggerSchedulingPhase1 = new VehStatsTriggerSchedulingPhase1("VehStatsTriggerSchedulingPhase1",topologyID);
        reconfigurationTriggers.add(VehStatsTriggerSchedulingPhase1);

//        ReconfigurationTrigger VehStatsTriggerSchedulingPhase3 = new VehStatsTriggerSchedulingPhase3("VehStatsTriggerSchedulingPhase3",topologyID);
//        reconfigurationTriggers.add(VehStatsTriggerSchedulingPhase3);

        reconfigurationEventObserver = new ReconfigurationEventObserver(dataCenterManager, cloudMetricsMessageParser.getMetricsDBReader(),reconfigurationTriggers,executionPlan,taskDispatcher);

        reconfigurationEventObserver.start();
    }
}
