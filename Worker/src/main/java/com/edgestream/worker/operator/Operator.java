package com.edgestream.worker.operator;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.io.MessageConsumerClient;
import com.edgestream.worker.io.MessageProducerClient;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.metrics.metricscollector2.operator.OperatorCollector;
import com.edgestream.worker.metrics.metricscollector2.reconfiguration.ReconfigurationCollector;
import com.edgestream.worker.metrics.metricscollector2.stream.StreamCollector;
import org.apache.activemq.artemis.api.core.ActiveMQException;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;

public class Operator implements Serializable, SingleTupleProcessing, SingleTupleEmitter {

    public MetricsCollector3 metricsCollector;
    public OperatorCollector operatorCollector;
    public StreamCollector streamCollector;
    public ReconfigurationCollector reconfigurationCollector;
    private MessageProducerClient messageProducerClient;
    private MessageConsumerClient messageConsumerClient;
    private final OperatorID operatorID;
    private final String inputType;
    private HashMap<String,MessageProducerClient> producerClientByOutputTypeMap;
    private String operatorAddress;
    private String operatorQueueName;
    private String attributeAddress;
    private boolean warmupPhase = true; // the operator always starts up in the warmup phase
    private boolean canEmit = true;
    private boolean tupleLiveStatus = true;
    public ZonedDateTime tupleProcessStartTime;
    private ZonedDateTime tupleProcessEndTime;
    private Spout spout;  //The operator can also have a spout



    /***
     * This constructor should be used when this operator will have only one output type.
     * @param messageProducerClient
     * @param operatorID
     * @param inputType
     */
    public Operator (MessageProducerClient messageProducerClient, OperatorID operatorID, String inputType){

        this.messageProducerClient = messageProducerClient;
        this.operatorID = operatorID;
        this.inputType = inputType;

    }

    /*** TODO: implement the warmer to disable and enable the output to avoid calling the metrics collector
     * This constructor should be used when this operator will output more than one Tuple type, a HashMap of producerClients is then passed to the constructor
     * @param messageProducerClient
     * @param operatorID
     * @param inputType
     */
    public Operator (HashMap<String,MessageProducerClient> messageProducerClient, OperatorID operatorID, String inputType) {

        this.producerClientByOutputTypeMap = messageProducerClient;
        this.operatorID = operatorID;
        this.inputType = inputType;
    }


    /***
     * This constructor should be used when you want to create the operator before creating the producer client.
     * You must call the {@link #setActiveMessageProducerClient} and {@link #setMetricsCollector}
     * function before you use this operator. This is done in EdgeStreamApplicationContext.
     * @param operatorID
     * @param inputType
     */
    public Operator (OperatorID operatorID, String inputType, MetricsCollector3 metricsCollector){
        this.operatorID = operatorID;
        this.inputType = inputType;
        this.setMetricsCollector(metricsCollector);
    }


    public MessageConsumerClient getMessageConsumerClient() {
        return messageConsumerClient;
    }

    public void setMetricsCollector(MetricsCollector3 metricsCollector) {
        System.out.println("Initialising metrics collector for operator");
        this.metricsCollector = metricsCollector;
        this.operatorCollector = this.metricsCollector.getOperatorCollector();
        this.streamCollector = this.metricsCollector.getStreamCollector();
        this.reconfigurationCollector = this.metricsCollector.getReconfigurationCollector();
    }

    public void setMessageConsumerClient(MessageConsumerClient messageConsumerClient) {
        this.messageConsumerClient = messageConsumerClient;
    }

    public OperatorID getOperatorID() {

        return operatorID;
    }


    public String getOperatorAddress() {
        return operatorAddress;
    }

    public void setOperatorAddress(String operatorAddress) {
        this.operatorAddress = operatorAddress;
    }

    public String getOperatorQueueName() {
        return operatorQueueName;
    }

    public void setOperatorQueueName(String operatorQueueName) {
        this.operatorQueueName = operatorQueueName;
    }

    public String getAttributeAddress() {
        return attributeAddress;
    }

    public void setAttributeAddress(String attributeAddress) {
        this.attributeAddress = attributeAddress;
    }

    public synchronized void setActiveMessageProducerClient(MessageProducerClient messageProducerClientActive){
        this.messageProducerClient = messageProducerClientActive;

    }


    //TODO: update this to support the warmer enable and disable features
    public synchronized void setActiveMessageProducerClient(HashMap<String,MessageProducerClient> producerClientByOutputTypeMap){
        this.producerClientByOutputTypeMap = producerClientByOutputTypeMap;

    }


    public MessageProducerClient getMessageProducerClient() {

        return messageProducerClient;
    }


    public MessageProducerClient getMessageProducerClient(String outputType) {

        //returns the producer for the given output type
        return this.producerClientByOutputTypeMap.get(outputType);
    }

    public void collectInputMetrics(Tuple tuple) {
        String transfer = tuple.getTupleHeader().getTimestampTransfer();
        String previous_operator = tuple.getTupleHeader().getPreviousOperator();
        long inputEventSize = Long.valueOf(tuple.getPayloadAsByteArray().length);
        this.streamCollector.add(previous_operator, transfer, inputEventSize);
        this.operatorCollector.addInput(inputEventSize);
        this.tupleProcessStartTime = ZonedDateTime.now();
    }

    public void collectOutputMetrics(Tuple tuple, String timeStamp) {
        long outputEventSize = Long.valueOf(tuple.getPayloadAsByteArray().length);
        tupleProcessEndTime = ZonedDateTime.now();
        long processingDuration = Duration.between(tupleProcessStartTime, tupleProcessEndTime).toNanos();
        this.operatorCollector.addOutput(outputEventSize, timeStamp, processingDuration);
    }


    public void processElement(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey)  {
        if(tuple.isStableMarker()){
            System.out.println("[Operator]" + "Stable marker received.....");
            this.disableWarmUpPhase();
            this.reset(); //reset the internal state of this operator
            this.enableEmitter(); //allow this operator to start emitting instead of dropping tuples

        }else {
            collectInputMetrics(tuple);
            processTuple(tuple, tupleID, tupleInternalID, producerId, timeStamp, tupleOrigin, inputKey);
        }
    }

    @Override
    public void emit(Tuple tupleToEmit, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
        getMessageProducerClient().onRequestToSend(tupleToEmit, tupleID, tupleInternalID, producerId, timeStamp, getOperatorID().getOperatorID_as_String(), tupleOrigin, inputKey,canEmit());
        // Stateless operators need to keep forwarding tuples during warmup, but
        // they should not be accounted for in metrics collector.
        if (tupleToEmit.isLiveTuple()) {
            collectOutputMetrics(tupleToEmit, timeStamp);
        }
    }

    public void emitForSink(Tuple tuple, String timeStamp) {
        collectOutputMetrics(tuple, timeStamp);
    }


    public String getInputType() {
        return inputType;
    }

    /**
     * Call this function to reset any internal state of the operator
     */
    public void reset(){

    }

    void printWarmUpState(){
        System.out.println("From Operator: warm up state is: [" +warmupPhase +"]");
    }

    public void enableWarmUpPhase(){
        System.out.println("Enabling operator warmup phase....");
        warmupPhase = true;
        printWarmUpState();
        disableEmitter();
        getMessageProducerClient().setOperatorIsInWarmUpPhase(true);//notify the producer that metrics can now be emitted
        getMessageConsumerClient().setOperatorIsInWarmUpPhase(true);//notify the consumer that metrics can now be emitted

    }

    public void disableWarmUpPhase(){
        System.out.println("Disabling operator warmup phase....");
        warmupPhase = false;
        enableEmitter();
        printWarmUpState();
        getMessageProducerClient().setOperatorIsInWarmUpPhase(false);//notify the producer that metrics should NOT be emitted
        getMessageConsumerClient().setOperatorIsInWarmUpPhase(false);//notify the consumer that metrics should NOT be emitted

    }

    public boolean isInWarmUpPhase(){
        return warmupPhase;
    }

    public void enableEmitter() {
        if (!this.canEmit) {
            System.out.println("From Operator: Enabling operator emitter....");
            this.canEmit = true;
        } else {
            System.out.println("From Operator: Emitter already ENABLED....");
        }
    }


    public void disableEmitter(){
        if(this.canEmit){
            System.out.println("From Operator: Disabling operator emitter....");
            this.canEmit = false;
        }else{
            System.out.println("From Operator: Emitter already DISABLED....");
        }
    }

    public boolean canEmit(){
        return canEmit;
    }

    public void updateTupleLiveSetting(boolean tupleStatus){
        tupleLiveStatus = tupleStatus;
        System.out.println("From Operator: this operator will emit live tuples?: ["+ tupleLiveStatus + "]");

    }

    @Override
    public void processTuple(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
    }


    /****************************************************************************
     *
     *          Spout implementation
     *
     ****************************************************************************/

    public Spout getSpout() {
        return spout;
    }

    public void setSpout(Spout spout) {
        this.spout = spout;
    }
}
