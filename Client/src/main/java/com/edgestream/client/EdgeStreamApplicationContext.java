package com.edgestream.client;

import com.edgestream.client.management.EdgeStreamApplicationManagementListener;
import com.edgestream.client.warmer.ProducerPubReceiver;
import com.edgestream.worker.io.artemis.*;
import com.edgestream.worker.io.MessageConsumerClient;
import com.edgestream.worker.io.MessageProducerClient;
import com.edgestream.worker.io.zeroMQ.ZeroMQProducerClient;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector2;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.metrics.metricscollector2.io.ZMQContainerMetricsService;
import com.edgestream.worker.metrics.metricscollector2.derby.ContainerMetricsDerbyDB;
import com.edgestream.worker.operator.Operator;
import com.edgestream.worker.runtime.task.model.Task;
import com.edgestream.worker.runtime.task.model.TaskWarmerStrategy;
import org.zeromq.ZContext;

import java.util.HashMap;

public class EdgeStreamApplicationContext {


    private final EdgeStreamApplicationConfig edgeStreamApplicationConfig;
    private final Operator operator;
    private final Task task;
    private final MessageConsumerClient mcc;
    private ActiveMQTMtoOperatorManagementProducer activeMQTMtoOperatorManagementProducer;
    private ActiveMQOPtoOperatorManagementProducer activeMQOPtoOperatorManagementProducer;
    private EdgeStreamApplicationManagementListener edgeStreamApplicationManagementListener;
    private int MPC_counter = 1;


    private final ZContext context = new ZContext();
    private ProducerPubReceiver producerPubReceiver;
    private boolean contextReadyToStart = false;
    private boolean sendReadyMessageToPredecessor = false;
    private final MetricsCollector3 metricsCollector;


    /**New ZMQ Metrics DB setup*/
    private final ContainerMetricsDerbyDB containerMetricsDerbyDB = new ContainerMetricsDerbyDB(); //This creates and sets up the in memory metrics DB
    private ZMQContainerMetricsService zmqContainerMetricsService;
    private final String metricsInputEventPortNumber = "4001";
    private final String metricsOutputEventPortNumber = "4002";




    public EdgeStreamApplicationContext(EdgeStreamApplicationConfig edgeStreamApplicationConfig, Operator operator, MetricsCollector3 metricsCollector) {
        this.edgeStreamApplicationConfig = edgeStreamApplicationConfig;
        this.operator = operator;
        this.metricsCollector = metricsCollector;

        // The zmqMetricsConsumer starts the threads that will receive and insert the values into the db
        //this.zmqContainerMetricsService = new ZMQContainerMetricsService(containerMetricsDerbyDB,metricsInputEventPortNumber,metricsOutputEventPortNumber);

        //TODO: update the producerClients to send their metrics to the ZMQ ports instead of calling the MetricsCollector2 directly


        setInitialContextState();
        startWarmerDummyReceivers();
        startOPtoOPManagementListener();
        createTMtoOPManagementProducer();
        createOPtoOPManagementProducer();

        String taskID = "Task_001"; //TODO: replace with values coming from user input
        this.task = new Task(taskID,operator);

        HashMap<String,String> outputPorts = edgeStreamApplicationConfig.getTypeToPortMap();

        //for when this operator will have only ONE output type
        if(outputPorts.size() == 1) {
            String producerID = "Producer_Client_00"+ MPC_counter;
            MPC_counter++;
            MessageProducerClient mpc = buildMessageProducerClient(
                      this.edgeStreamApplicationConfig.getOperatorOutputAddress()
                    , producerID
                    , this.edgeStreamApplicationConfig.getOperatorOutputBrokerIP()
                    , this.metricsCollector, edgeStreamApplicationConfig.getProducerBatchSize()
                    , this.edgeStreamApplicationConfig.getProducerBufferSize()
                    , "8000"
                    , this.edgeStreamApplicationConfig.getSuccessorIP()
                    , this.edgeStreamApplicationConfig.getTopologyID()
                    , metricsOutputEventPortNumber);

            this.operator.setActiveMessageProducerClient(mpc);

        }

        //for when this operator will have multiple output types
        HashMap<String,MessageProducerClient> messageProducerClients = new HashMap<>();
        if(outputPorts.size() >1){
            for(String outputType :outputPorts.keySet()){
                String producerID = "Producer_Client_001_" + outputType;
                messageProducerClients.put(outputType,buildMessageProducerClient(edgeStreamApplicationConfig.getOperatorOutputAddress(), producerID
                        , edgeStreamApplicationConfig.getOperatorOutputBrokerIP(), metricsCollector, edgeStreamApplicationConfig.getProducerBatchSize()
                        , edgeStreamApplicationConfig.getProducerBufferSize(), outputPorts.get(outputType), edgeStreamApplicationConfig.getSuccessorIP(), edgeStreamApplicationConfig.getTopologyID(), metricsOutputEventPortNumber));

            }
            this.operator.setActiveMessageProducerClient(messageProducerClients);
        }



        String consumerClientID = "Consumer_Client_001"; //TODO: replace with values coming from user input
        this.mcc = buildMessageConsumerClient(consumerClientID,edgeStreamApplicationConfig.getOperatorInputQueue(),this.task
                ,edgeStreamApplicationConfig.getOperatorInputBrokerIP(),edgeStreamApplicationConfig.getConsumerBatchSize()
                ,edgeStreamApplicationConfig.getConsumerBufferSize(), edgeStreamApplicationConfig.getPredecessorInputMethod()
                ,edgeStreamApplicationConfig.getPredecessorIP(),edgeStreamApplicationConfig.getPredecessorPort() ,edgeStreamApplicationConfig.getWarmupFlag(),edgeStreamApplicationConfig.getWarmupContainerIP());

        this.operator.setMessageConsumerClient(this.mcc);

        setWarmUpStrategy();
    }


    public boolean isSendReadyMessageToPredecessor() {
        return sendReadyMessageToPredecessor;
    }

    private void setInitialContextState(){
        if(edgeStreamApplicationConfig.getWaitForSuccessorReadyMessage().equalsIgnoreCase("DO_NOT_WAIT")){
            startContext();
        }

        if(edgeStreamApplicationConfig.getNotifyPredecessorWhenReady().equalsIgnoreCase("YES_NOTIFY")){
            sendReadyMessageToPredecessor = true;
        }
    }

    public synchronized void startContext(){
        contextReadyToStart = true;
    }


    public void startApplication(){

        mcc.startTask();
        //After the startTask() has returned here, the warm up phase is complete and the operator is connected to the broker and any live predecessor ZMQ ports

        // TODO: The context for Stateful operators is always launched in a READY state because
        //  they manage their own internal warmup phase from Pathstore, so this checking logic below is not needed.
        if(edgeStreamApplicationConfig.getWarmupFlag() == TaskWarmerStrategy.NO_STATEFUL_DUAL_ROUTED) {
            operatorIsReady();
        }

        if(edgeStreamApplicationConfig.getWarmupFlag() != TaskWarmerStrategy.NO_STATEFUL_DUAL_ROUTED) {
            operatorIsReady();
            if (mcc.getTaskWarmerStrategy() == TaskWarmerStrategy.YES_SAME_CONTAINER_TYPE_SWITCH_ZMQ ||
                    mcc.getTaskWarmerStrategy() == TaskWarmerStrategy.YES_PREDECESSOR_CONTAINER) {
                System.out.println("[EdgeStreamApplicationContext]:  TaskWarmerStrategy is [" + mcc.getTaskWarmerStrategy() + "] so we will send a switch output request to the predecessor");
                switchOutputOfPredecessor();
            }
        }

    }

    private void operatorIsReady(){

        //1. first check to see if the context is ready. Block until it is
        while(!contextReadyToStart){
            try {
                System.out.println("[EdgeStreamApplicationContext]: Context is not ready, waiting on successor operators to send their ready message");
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("[EdgeStreamApplicationContext]: Context is ready to start...");
        //check to see if we need to send a ready msg to our predecessor
        if(isSendReadyMessageToPredecessor()){
            this.activeMQOPtoOperatorManagementProducer.sendReadyMessageToPredecessor( edgeStreamApplicationConfig.getReconfigurationPlanID()
                    , operator.getOperatorID().getOperatorID_as_String()
                    , edgeStreamApplicationConfig.getWarmupContainerOperatorID()
                    , edgeStreamApplicationConfig.getPredecessorIP());
        }
        //now send a ready msg to the task manager
        this.activeMQTMtoOperatorManagementProducer.sendReadyStatusMessage(edgeStreamApplicationConfig.getReconfigurationPlanID(),operator.getOperatorID().getOperatorID_as_String());
    }


    private void startOPtoOPManagementListener(){

        this.edgeStreamApplicationManagementListener = new EdgeStreamApplicationManagementListener(
                this.edgeStreamApplicationConfig.getOPtoOPManagementFQQN()
                ,this.edgeStreamApplicationConfig.getMetrics_broker_IP_address()
        ,this);

        this.edgeStreamApplicationManagementListener.start();
    }


    public void switchOutputToZMQ(String successorIP) {

        if(edgeStreamApplicationConfig.getSuccessorIP().equalsIgnoreCase("FUSED")){ //FUSED means a successor operator container will connect to this one, hence why we don't pass in an actual successor IP
            System.out.println("[EdgeStreamApplicationContext]: This operator is already writing out to zmq, will ignore this request");
        }else{
            System.out.println("[EdgeStreamApplicationContext]: This operator is will be switched to write its output instead to ZMQ");
            updateOperatorMPC(successorIP);
        }

    }


    //TODO: verify if we need to set this successor IP because all we are doing is changing the output type of this container,
    // and this doesn't make sense if have multiple successor(s) replica(s)
    protected void updateOperatorMPC(String successorIP){


        this.operator.getMessageProducerClient().tearDown();


        edgeStreamApplicationConfig.setSuccessorIP(successorIP);

        String producerID = "Producer_Client_00"+ MPC_counter;
        MPC_counter++;
        MessageProducerClient mpc = buildMessageProducerClient(
                edgeStreamApplicationConfig.getOperatorOutputAddress()
                , producerID
                , edgeStreamApplicationConfig.getOperatorOutputBrokerIP()
                , metricsCollector //TODO: This is owned by the EdgeStreamApplicationContext context so it should be switchable, but this needs to be verified
                , edgeStreamApplicationConfig.getProducerBatchSize()
                , edgeStreamApplicationConfig.getProducerBufferSize()
                , "8000"
                , edgeStreamApplicationConfig.getSuccessorIP()
                , edgeStreamApplicationConfig.getTopologyID(), metricsOutputEventPortNumber);

        mpc.setOperatorIsInWarmUpPhase(false); // the operator is already hot, we are just changing its emitter type to ZMQ

        this.operator.setActiveMessageProducerClient(mpc);

    }




    /******************************************************************************
     * All message producers and consumers start with the warm up state set to true.
     * This method will disable the startup state if a warm up is not needed.
     * Both on the consumer and producer side.
     *****************************************************************************/
    public void setWarmUpStrategy(){
        // The default is to be in a warm up state
        this.task.enableBoundOperatorWarmupPhase();

        // This is an initial cloud deployment or p2p connection so no warmup is needed, so we disable it
        if(edgeStreamApplicationConfig.getWarmupFlag() == TaskWarmerStrategy.NO_INITIAL_DEPLOYMENT){
                //|| edgeStreamApplicationConfig.getWarmupFlag() == TaskWarmerStrategy.NO_ZMQ_P2P){  // or this is a p2p connection so no warmup is needed
            this.task.disableBoundOperatorWarmupPhase();
        }

        //There is no successor IP meaning that this operator writes its output to the broker
        if(edgeStreamApplicationConfig.getSuccessorIP().equals("NONE")
                && (edgeStreamApplicationConfig.getWarmupFlag() == TaskWarmerStrategy.NO_ZMQ_P2P
                    || edgeStreamApplicationConfig.getWarmupFlag() == TaskWarmerStrategy.YES_SAME_CONTAINER_TYPE_SWITCH_AMQ
                    || edgeStreamApplicationConfig.getWarmupFlag() == TaskWarmerStrategy.YES_SAME_CONTAINER_TYPE_SWITCH_ZMQ
                    || edgeStreamApplicationConfig.getWarmupFlag() == TaskWarmerStrategy.YES_ACTIVEMQ
                    || edgeStreamApplicationConfig.getWarmupFlag() == TaskWarmerStrategy.NO_STATEFUL_DUAL_ROUTED)
                    ){
            this.task.disableBoundOperatorEmitter(); // this disables the emit function inside the operator. Only a tuple with the StableMarker flag set to TRUE with enable the emit
        }

        //This operator has a successor and it will wait for a stable marker to enable a chain starting from the broker.
        if(edgeStreamApplicationConfig.getWarmupFlag() == TaskWarmerStrategy.YES_ACTIVEMQ && !edgeStreamApplicationConfig.getSuccessorIP().equals("NONE")
                || (!edgeStreamApplicationConfig.getSuccessorIP().equals("NONE") && edgeStreamApplicationConfig.getWarmupFlag() == TaskWarmerStrategy.NO_ZMQ_P2P)){
            this.task.enableBoundOperatorEmitter();//enable the emitter so so tuples can flow but keep the warm up state so the mpc and mcc clients can know to skip the metrics collection
        }
    }


    private void switchOutputOfPredecessor(){

        this.activeMQOPtoOperatorManagementProducer.sendSwitchPredecessorOutputToZMQMessage(
                edgeStreamApplicationConfig.getReconfigurationPlanID()
                , operator.getOperatorID().getOperatorID_as_String()
                , edgeStreamApplicationConfig.getWarmupContainerOperatorID()
                , edgeStreamApplicationConfig.getPredecessorIP()
        );
    }




    public String getOperatorID() {
        return operator.getOperatorID().getOperatorID_as_String();
    }

    private void createTMtoOPManagementProducer(){

        String queue = edgeStreamApplicationConfig.getTMToOPManagementFQQN();
        System.out.println("Queue Name: [" + queue +"]");
        String ip = edgeStreamApplicationConfig.getOperatorOutputBrokerIP();
        System.out.println("ip: [" + ip + "]");
        this.activeMQTMtoOperatorManagementProducer =  new ActiveMQTMtoOperatorManagementProducer(queue,ip);
    }


    private void createOPtoOPManagementProducer() {
        String queue = edgeStreamApplicationConfig.getOPtoOPManagementFQQN();
        System.out.println("Queue Name: [" + queue +"]");
        String ip = edgeStreamApplicationConfig.getOperatorOutputBrokerIP();
        System.out.println("ip: [" + ip + "]");
        this.activeMQOPtoOperatorManagementProducer =  new ActiveMQOPtoOperatorManagementProducer(queue,ip);
    }


    /**
     * A helper method to build the Producer client. It will make a connection to the broker and return a usable Producer client
     * @param output_FFQN
     * @param producer_client_iD
     * @param metricsCollector
     * @param metricsOutputEventPortNumber
     * @return
     */
    private MessageProducerClient buildMessageProducerClient(String output_FFQN, String producer_client_iD, String broker_ip
            , MetricsCollector3 metricsCollector, int batchSize, int bufferSize, String outputPort, String successorIP, String topologyID, String metricsOutputEventPortNumber)  {

        MessageProducerClient messageProducerClient = null;

        try {

            /**If this operator will write its output directly to ActiveMQ use ActiveMQMessageProducerClient, else, use the zero mq producer and pass it the successorIP,
             * which is the ip address of an already active consumer that will receive the output of this operator
             * */
            System.out.println(String.format("EdgeStreamApplicationContext]: Choosing ActiveMQMessageProducerClient [%s] [%s]",producer_client_iD,output_FFQN));
            //output_FFQN = "merlin_default";  //TODO: to force the tuples to go to the local broker instead of the no match address TO BE REMOVED
            //messageProducerClient = new ActiveMQMessageProducerClient(producer_client_iD, output_FFQN, broker_ip,metricsCollector,batchSize,bufferSize,topologyID,metricsOutputEventPortNumber);


            //operator will write out to the broker
            if(successorIP.equalsIgnoreCase("NONE")){
                messageProducerClient = new ActiveMQMessageProducerClient(producer_client_iD, output_FFQN, broker_ip,metricsCollector,batchSize,bufferSize,topologyID,metricsOutputEventPortNumber);
            }
            //this operator is expected to be fused with a future successor
            if(successorIP.equalsIgnoreCase("FUSED"))            {
                messageProducerClient = new ZeroMQProducerClient(producer_client_iD, outputPort, metricsCollector, batchSize,metricsOutputEventPortNumber);
            }
            // this producer has been started after the consumers so it must be the one to make a connection to a given consumer.
            // The EdgeStream framework will provide the IP of a specific consumer
            if(!successorIP.equalsIgnoreCase("FUSED") && !successorIP.equalsIgnoreCase("NONE")){
                messageProducerClient = new ZeroMQProducerClient(producer_client_iD, outputPort, metricsCollector, batchSize, successorIP,metricsOutputEventPortNumber);  //TODO: need to add support for binding late producers to multiple successor containers
            }


        }catch (Exception e){
            e.printStackTrace();
        }

        return messageProducerClient;

    }


    /**
     * A helper method to build the Consumer client. It will make a connection to the broker and return a usable Consumer client
     * @param consumer_client_iD
     * @param input_FFQN
     * @param taskToRun
     * @param predecessorInputMethod
     * @param predecessorIP
     * @param predecessorPort
     * @param warmupFlag
     * @return
     * @throws Exception
     */
    private MessageConsumerClient buildMessageConsumerClient(String consumer_client_iD, String input_FFQN, Task taskToRun, String broker_ip
            , int batchSize, int bufferSize, String predecessorInputMethod, String predecessorIP, String predecessorPort , TaskWarmerStrategy warmupFlag, String warmupContainerIP )  {

        MessageConsumerClient messageConsumerClient =null;
        try {
            System.out.println("EdgeStreamApplicationContext]: Choosing ActiveMQToZeroMQSwitchableMessageConsumerClient");
            //consumer starts in warmer mode by consuming from the input port of a container of the same type
            //after the warmup ends the consume switches to consume from the broker
            if(warmupFlag == TaskWarmerStrategy.YES_SAME_CONTAINER_TYPE_SWITCH_AMQ
                    || warmupFlag == TaskWarmerStrategy.YES_SAME_CONTAINER_TYPE_SWITCH_ZMQ
                    || warmupFlag == TaskWarmerStrategy.YES_PREDECESSOR_CONTAINER){

                messageConsumerClient = new ActiveMQToZeroMQSwitchableMessageConsumerClient(consumer_client_iD, input_FFQN, taskToRun, broker_ip
                        , batchSize,bufferSize,predecessorInputMethod,predecessorIP,predecessorPort,warmupFlag,warmupContainerIP,metricsInputEventPortNumber);
            }else{// TODO: stateful operators use [ActiveMQToZeroMQMessageConsumerClientV2] because only ONE replica in a DC is currently supported
                messageConsumerClient = new ActiveMQToZeroMQMessageConsumerClientV2(consumer_client_iD, input_FFQN, taskToRun, broker_ip
                        , batchSize,bufferSize,predecessorInputMethod,predecessorIP,predecessorPort, metricsInputEventPortNumber);
            }

        }catch(Exception e){
            e.printStackTrace();
        }

        return messageConsumerClient;
    }



    private void startWarmerDummyReceivers() {
        System.out.println("[EdgeStreamApplicationContext]: Creating dummy [Producer] pub receiver ...."); //this just receives a copy of the tuple and drops it
        producerPubReceiver = new ProducerPubReceiver(this.context, "PRODUCER_DUMMY_1");
        producerPubReceiver.start();
    }
}
