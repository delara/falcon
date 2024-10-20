package com.edgestream.worker.runtime.topology;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.common.TupleHeader;
import com.edgestream.worker.operator.Operator;
import com.edgestream.worker.runtime.reconfiguration.state.RoutingKey;
import com.edgestream.worker.runtime.task.broker.TaskBrokerConfig;
import com.edgestream.worker.runtime.topology.exception.TopologyException;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.commons.lang3.SerializationUtils;

import java.io.ByteArrayInputStream;
import java.time.ZonedDateTime;
import java.util.ArrayList;

public class TopologyAttributeAddressManager {

    private RoutingKey routingKey;
    private final String attributeAddress;
    private final ArrayList<Operator> operatorsOnThisAddress = new ArrayList<>();
    private AddressControl addressController;
    private boolean hasDualRoutingEnabled = false;
    private Tuple markerTuple;
    private final TopologyOperatorAddressManager boundTopologyOperatorAddressManager;
    private boolean isNewAttributeAddress = true;
    private String attributeQueueName = "NOT_SET";
    private boolean isCold = true;
    private final Kryo kryo = new Kryo(); //KRYO CHANGE


    public TopologyAttributeAddressManager(RoutingKey routingKey, String attributeAddress,TopologyOperatorAddressManager boundTopologyOperatorAddressManager) {

        this.routingKey = routingKey;
        this.attributeAddress = attributeAddress;
        this.boundTopologyOperatorAddressManager = boundTopologyOperatorAddressManager;
        if(routingKey.hasSyncTuple()){
            enableDualRouting();
        }

        kryo.register(TupleHeader.class);//KRYO CHANGE
        kryo.register(byte[].class); //KRYO CHANGE
        kryo.register(Tuple.class); //KRYO CHANGE

    }



    /**
     * TODO: We currently use a single queue for this address. This could become a bottle neck.
     *  One queue per operator will have better performance.
     *
     * @return
     */


    private String getFQQN(){

        return getAttributeAddress() + "::" + getAttributeQueueName();

    }

    public boolean isCold() {
        return isCold;
    }

    public void setAddressToHot(){
        isCold = false;
    }

    public String getAttributeQueueName() {
        return attributeQueueName;
    }

    public void setAttributeQueueName(String attributeQueueName) {
        this.attributeQueueName = attributeQueueName;
    }

    public boolean isNewAttributeAddress() {
        return isNewAttributeAddress;
    }

    private void attributeAddressSetupComplete(){
        this.isNewAttributeAddress = false;
    }

    public TopologyOperatorAddressManager getBoundTopologyOperatorAddressManager(){
        return boundTopologyOperatorAddressManager;
    }

    private TaskBrokerConfig getBoundTaskBrokerConfig(){

        return this.getBoundTopologyOperatorAddressManager().getBoundTopology().getBoundTaskDriver().getTaskBrokerConfig();
    }

    public void setAddressController(AddressControl addressController){

        this.addressController = addressController;

    }

    private void setMarkerTuple(Tuple markerTuple){

        this.markerTuple = markerTuple;
    }

    private Tuple getMarkerTuple(){

        return this.markerTuple;

    }

    public void pauseAttributeAddress(){
        try {
            System.out.println("[TopologyAttributeAddressManager] Pausing address: [" + attributeAddress +"]");

            addressController.pause();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void resumeAttributeAddress(){
        try {
            System.out.println("[TopologyAttributeAddressManager] Resuming Attribute address: " + attributeAddress);

            addressController.resume();

            if(isNewAttributeAddress()) {
                attributeAddressSetupComplete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public boolean hasDualRoutingEnabled() {
        return hasDualRoutingEnabled;
    }


    private void enableDualRouting(){

        hasDualRoutingEnabled = true;

    }


    private void disableDualRouting(){

        hasDualRoutingEnabled = false;

    }

    public void installDualRouteToTopologyNoMatch(){
        getBoundTopologyOperatorAddressManager().getBoundTopology().getBoundTaskDriver().getTaskBrokerConfig().createDivertFromAttributeAddressToNoMatchAddress(this);
        enableDualRouting();

    }

    public void uninstallDualRouteToTopologyNoMatch(){

        if(hasDualRoutingEnabled()) {
            System.out.println("[TopologyAttributeAddressManager] Stateless dual routing complete, will now uninstall.......");

            //1. disable flag
            disableDualRouting();

            //2. pause
            pauseAttributeAddress();

            //3.destroy divert the copies tuples to be sent to the original operator
            getBoundTopologyOperatorAddressManager().getBoundTopology().getBoundTaskDriver().getTaskBrokerConfig().destroyDivert(getDualRouteDivertName());
            //getBoundTopologyOperatorAddressManager().getBoundTopology().getBoundTaskDriver().getTaskBrokerConfig().purgeQueue(attributeAddress,attributeQueueName);

            //4. Inject stable marker
            injectStableMarker(attributeAddress);

            //5. set address to hot
            setAddressToHot();

            //6. resume
            resumeAttributeAddress();
        }else{
            throw new TopologyException("[TopologyAttributeAddressManager] Attempting to uninstall dual route: ["+ getDualRouteDivertName() + "] but dual route is already uninstalled, something has gone wrong, check your logic", new Throwable());
        }

    }



    public void shutDownDualRouting(){

        System.out.println("[TopologyAttributeAddressManager] Dual routing shutdown signal triggered.....");
        //1. pause the attribute address
        pauseAttributeAddress();

        //2. remove divert
        getBoundTopologyOperatorAddressManager().getBoundTopology().getBoundTaskDriver().getTaskBrokerConfig().destroyDivert(getDualRouteDivertName());

        //3. signal shutdown
        sendShutDownSignal();

        //4. disable dual routing flag
        disableDualRouting();

        setAddressToHot();

        //5. resume address
        resumeAttributeAddress();

    }

    public String getDualRouteDivertName(){

        return  "dual_routing_divert_for" + routingKey.routingKeyAttributeQueueName();
    }

    public String getDivertName(){

      return  "divert_for_" + routingKey.routingKeyAttributeQueueName();
    }

    public String getFilterString(){

        return  routingKey.getFilterString();


    }
    public String getRoutingName(){
        return "forward_tuples_to_attribute_address_" + attributeAddress;

    }

    public RoutingKey getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(RoutingKey routingKey) {
        this.routingKey = routingKey;
    }

    public String getAttributeAddress() {
        return attributeAddress;
    }

    public ArrayList<Operator> getOperatorsOnThisAddress() {
        return operatorsOnThisAddress;
    }

    public void addOperator(Operator operator){
        operatorsOnThisAddress.add(operator);

    }


    public void removeOperator(Operator operatorToFind){

        boolean foundOperator = false;
        for(Operator op: this.operatorsOnThisAddress){
            if(operatorToFind.getOperatorID().getOperatorID_as_String().equalsIgnoreCase(op.getOperatorID().getOperatorID_as_String())){
                operatorsOnThisAddress.remove(op);
                foundOperator = true;
            }
        }

        if(!foundOperator){

            throw new TopologyException("[TopologyAttributeAddressManager] Could not find operator: "+ operatorToFind.getOperatorID().getOperatorID_as_String()  + " on attribute address : " +  attributeAddress, new Throwable());
        }

    }

    Topology getBoundTopology(){

        return this.getBoundTopologyOperatorAddressManager().getBoundTopology();
    }

    private void sendShutDownSignal(){
        injectTupleMarker(this.attributeAddress, routingKey.getStableTuple());

    }



    public void beginStatefulOperatorBackupProcess(Tuple syncTuple){

        pauseAttributeAddress();
        setMarkerTuple(syncTuple);
        sendSourceBackupSignal(syncTuple);
        resumeAttributeAddress();

    }


    private void sendSourceBackupSignal(Tuple syncTuple){

        injectTupleMarker(this.attributeAddress, syncTuple);
    }















    /****************************************************************************
     *
     *
     *                  Tuple injectors
     *                      |
     *                      |
     *                      V
     *
     *****************************************************************************/


    /**
     * For stateless operators, used to send a reset Tuple to the NEW operator replica AFTER the warmup is completed
     * @param destinationFQQN
     */
    private void injectStableMarker(String destinationFQQN){

        //Used to send tuples into the data stream
        ClientProducer producer = null;
        ClientSession session = null;
        ServerLocator locator;
        int confirmationWindowSize = 3000000;
        String connection_string = "tcp://"+ getBoundTaskBrokerConfig().getBroker_ip() + ":61616";

        try {

            locator = ActiveMQClient.createServerLocator(connection_string);
            locator.setConfirmationWindowSize(confirmationWindowSize);
            ClientSessionFactory factory = locator.createSessionFactory();
            session = factory.createSession();
            session.setSendAcknowledgementHandler(new MySendAcknowledgementsHandler());


            System.out.println("[TopologyAttributeAddressManager] Connected to Artemis Broker");
            System.out.println("[TopologyAttributeAddressManager] Injecting stable marker (a reset tuple) tuple to: " + destinationFQQN);

            //session.start();
            producer = session.createProducer(destinationFQQN);


        } catch (Exception e) {
            e.printStackTrace();
        }


        //1. create the message object
        ClientMessage msg_to_send = session.createMessage(false);


        //2. set the timestamp
        msg_to_send.setTimestamp(System.currentTimeMillis());
        //3. set the tuple type (as an attribute)
        String timestampTransfer = ZonedDateTime.now().toString();

        Tuple tupleToSend = new Tuple();
        tupleToSend.setTupleHeader(new TupleHeader("none","none","none","none","none","none"));
        tupleToSend.setStableMarker(true);

        msg_to_send.putStringProperty("tupleType", "");
        msg_to_send.putStringProperty("timeStamp", ZonedDateTime.now().toString());
        msg_to_send.putStringProperty("timestampTransfer", timestampTransfer);
        msg_to_send.putStringProperty("previousOperator", "none");
        msg_to_send.putStringProperty("topologyID", getBoundTopology().getTopologyID());
        msg_to_send.putStringProperty("tupleID", "none");
        msg_to_send.putBooleanProperty("stableMarker", true);

        byte priority = 9;
        msg_to_send.setPriority(priority);







        //byte[] dataToSend = SerializationUtils.serialize(tupleToSend); //REPLACED WITH KRYO
        //attached the byte array to an input stream
        //ByteArrayInputStream bis = new ByteArrayInputStream(dataToSend); //REPLACED WITH KRYO

        Output output = new Output(1024, -1);  //KRYO CHANGE
        kryo.writeObject(output, tupleToSend); //KRYO CHANGE
        output.close();//KRYO CHANGE
        byte[] dataToSend = output.getBuffer(); //KRYO CHANGE

        //msg_to_send.setBodyInputStream(bis); //KRYO CHANGE

        msg_to_send.putBytesProperty("payload",dataToSend);


        //5. send the message to the queue(this will read the byte stream and put it into the ActiveMQ msg)
        try {
            producer.send(msg_to_send);
            session.commit();
        } catch (ActiveMQException e) {
            e.printStackTrace();
        }



    }


    /**
     * For stateful operators, it's used to send a Tuple to the source operator in the parent DC to trigger the backup process to BEGIN
     * @param destinationAddress
     * @param markerTuple
     */
    private void injectTupleMarker(String destinationAddress, Tuple markerTuple){

        //Used to send tuples into the data stream
        ClientProducer producer = null;
        ClientSession session = null;
        ServerLocator locator;
        int confirmationWindowSize = 3000000;
        String connection_string = "tcp://"+ getBoundTaskBrokerConfig().getBroker_ip() + ":61616";

        try {

            locator = ActiveMQClient.createServerLocator(connection_string);
            locator.setConfirmationWindowSize(confirmationWindowSize);
            ClientSessionFactory factory = locator.createSessionFactory();
            session = factory.createSession();
            session.setSendAcknowledgementHandler(new MySendAcknowledgementsHandler());


            System.out.println("[TopologyAttributeAddressManager] Connected to Artemis Broker");
            System.out.println("[TopologyAttributeAddressManager] Injecting Tuple to: [" + destinationAddress + "]");

            //session.start();
            producer = session.createProducer(destinationAddress);


        } catch (Exception e) {
            e.printStackTrace();
        }


        //1. create the message object
        ClientMessage msg_to_send = session.createMessage(false);



        //2. set the timestamp
        msg_to_send.setTimestamp(System.currentTimeMillis());
        //3. set the tuple type (as an attribute)
        String timestampTransfer = ZonedDateTime.now().toString();

        Tuple tupleToSend = markerTuple;

        msg_to_send.putStringProperty("tupleType", tupleToSend.getType());
        msg_to_send.putStringProperty("timeStamp", ZonedDateTime.now().toString());
        msg_to_send.putStringProperty("timestampTransfer", timestampTransfer);
        msg_to_send.putStringProperty("previousOperator", "none");
        msg_to_send.putStringProperty("topologyID", getBoundTopology().getTopologyID());
        msg_to_send.putStringProperty("tupleID", "none");


        if (tupleToSend.getKeyField() != null) {
            msg_to_send.putStringProperty(tupleToSend.getKeyField(),tupleToSend.getKeyValue());
        }

        /**for debugging only, not used by operator*/
        if (tupleToSend.isReconfigMarker()) {
            msg_to_send.putBooleanProperty("reconfigMarker", tupleToSend.isReconfigMarker());
        }

        if (tupleToSend.isStableMarker()) {
            msg_to_send.putBooleanProperty("stableMarker", tupleToSend.isStableMarker());
        }


        Output output = new Output(1024, -1);  //KRYO CHANGE
        kryo.writeObject(output, tupleToSend); //KRYO CHANGE
        output.close();//KRYO CHANGE
        byte[] dataToSend = output.getBuffer(); //KRYO CHANGE



        msg_to_send.putBytesProperty("payload",dataToSend);


        //5. send the message to the queue(this will read the byte stream and put it into the ActiveMQ msg)
        try {
            producer.send(msg_to_send);
            session.commit();
        } catch (ActiveMQException e) {
            e.printStackTrace();
        }


    }













    private class MySendAcknowledgementsHandler implements SendAcknowledgementHandler {
        @Override
        public void sendAcknowledged(final Message message) {
        }
    }


}
