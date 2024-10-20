package com.edgestream.clustermanager.broker;

import com.edgestream.worker.runtime.plan.Individual_Task;
import com.edgestream.worker.runtime.docker.DockerFileType;
import com.edgestream.worker.runtime.reconfiguration.common.DagElementType;
import com.edgestream.worker.runtime.reconfiguration.common.DagPlan;
import com.edgestream.worker.runtime.reconfiguration.execution.ExecutionType;
import com.edgestream.worker.runtime.reconfiguration.state.RoutingKey;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.*;
import java.util.*;

public class TQmanager {
    private final String server_address;
    private String address_name;
    private final ArrayList<String> queue_names;
    private ServerLocator locator;
    private final List<ClientMessage> messageList;
    private ClientSessionFactory factory;
    private ClientSession session;
    private ClientProducer producer;
    private ClientConsumer consumer;


    public TQmanager(String server_address) {
        this.server_address = server_address;
        this.queue_names = new ArrayList<String>();
        this.messageList = new ArrayList<ClientMessage>();
        try {
            this.locator = ActiveMQClient.createServerLocator(server_address);
            locator.setMinLargeMessageSize(250000);
            this.factory = locator.createSessionFactory();
            this.session = factory.createSession();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // create a producer attached to input address_name (also create the address)
    public void createProducer(String address_name){
        this.address_name = address_name;
        try{
            this.producer = session.createProducer(address_name);
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    // construct a ActiveMQ message for each input Individual_Task and add task_manager_id to queue_name list
    public void messageBuilder(Individual_Task input_IT, ArrayList<Individual_Task> individual_Tasks, DockerFileType dockerFileType, ArrayList<ImmutablePair> dag, DagPlan dagPlan, String reconfigurationPlanID, ExecutionType executionType) throws FileNotFoundException {

        //Find all operators that will be deployed with this operator on the same data center

        String operatorType = input_IT.getOperator_Input_Type();
        StringBuilder colocatedOperators = new StringBuilder();
        String destinationHostName = input_IT.getHost_Name();

        for (Individual_Task it : individual_Tasks){
            if(destinationHostName.equalsIgnoreCase(it.getHost_Name()) && !operatorType.equalsIgnoreCase(it.getOperator_Input_Type())){ // this operator will be located in the same datacenter
                colocatedOperators.append(it.getOperator_Input_Type());
            }
        }



        ClientMessage msg = this.session.createMessage(false);


        byte[] executionTypeAsByteArray = SerializationUtils.serialize(executionType);
        msg.putObjectProperty("executionType",executionTypeAsByteArray );


        msg.putStringProperty("Individual_Task_ID", input_IT.getIndividual_Task_ID());
        msg.putStringProperty("Task_Manager_ID", input_IT.getTask_Manager_ID());
        msg.putStringProperty("Host_Name", input_IT.getHost_Name());
        msg.putStringProperty("Host_Address", input_IT.getHost_Address());
        msg.putStringProperty("Host_Port", input_IT.getHost_Port());
        msg.putStringProperty("Topology", input_IT.getTopology_ID());
        msg.putStringProperty("DeploymentMethod", input_IT.getOperator_Mode());
        msg.putStringProperty("OperatorID", input_IT.getOperator_ID());
        msg.putStringProperty("inputType", input_IT.getOperator_Input_Type());
        msg.putStringProperty("OperatorType", input_IT.getOperator_Type());

        byte[] operatorPredecessorTypes = SerializationUtils.serialize(new ArrayList(input_IT.getOperator_Predecessor_IDs()));
        msg.putObjectProperty("OperatorPredecessorType",operatorPredecessorTypes );

        byte[] operatorSuccessorTypes = SerializationUtils.serialize(new ArrayList(input_IT.getOperator_Successor_IDs()));
        msg.putObjectProperty("OperatorSuccessorType",operatorSuccessorTypes );

        msg.putStringProperty("colocatedOperators", String.valueOf(colocatedOperators));

        //String dockerType = String.valueOf(dockerFileType);
        msg.putStringProperty("dockerFileType", String.valueOf(dockerFileType));

        byte[] dagAsByteArray = SerializationUtils.serialize(dag);
        msg.putObjectProperty("dag",dagAsByteArray );

        byte[] dagPlanAsByteArray = SerializationUtils.serialize(dagPlan);
        msg.putObjectProperty("dagPlan",dagPlanAsByteArray );

        msg.putStringProperty("reconfigurationPlanID", reconfigurationPlanID);


        /************************************************************************************
         * Stateful Operator configuration details
         ***********************************************************************************/
        byte[] getDagElementTypeAsByteArray = SerializationUtils.serialize(input_IT.getDagElementType());
        msg.putObjectProperty("DagElementType",getDagElementTypeAsByteArray);

        if(input_IT.hasBootstrapState()){
            msg.putBooleanProperty("hasBootstrapState",true);
            byte[] stateTransferPlanAsByteArray = SerializationUtils.serialize(input_IT.getStateTransferPlan());
            msg.putObjectProperty("stateTransferPlan",stateTransferPlanAsByteArray );
        }else{
            msg.putBooleanProperty("hasBootstrapState",false);
        }

        /**
         * DEBUG
         */
        if(input_IT.getDagElementType() == DagElementType.STATEFUL) {
            RoutingKey routingKey = (RoutingKey)input_IT.getStateTransferPlan().getOperatorStateObject(RoutingKey.class.getName());

            System.out.println("Creating operator setup task for: "
                    + input_IT.getOperator_ID()
                    + " "
                    + input_IT.getDagElementType()
                    + " with key value pairs: " + routingKey.routingKeyListAsString());
        }else{
            System.out.println("Creating operator setup task for: " + input_IT.getOperator_ID() + " " + input_IT.getDagElementType());
        }

        /************************************************************************************
         * Stateful Operator configuration END
         ***********************************************************************************/




        // duplication detection (once and only once)
        msg.putStringProperty("HDR_DUPLICATE_DETECTION_ID", input_IT.getIndividual_Task_ID());
        // put begin timestamp
        msg.putLongProperty("Job_Submission_Time", input_IT.getJob_Submission_Time());

        // update queue_name list
        if (this.queue_names.contains(input_IT.getTask_Manager_ID())){
            System.out.println("duplicated queue name");
        }else{
            this.queue_names.add(input_IT.getTask_Manager_ID());
        }

        // Load Operator
        try {
            // Create FileInputStream
            String Operator_Filepath = input_IT.getOperator_Path();
            File myJarFile = new File(Operator_Filepath);
            InputStream targetStream = new FileInputStream(myJarFile);
            // Put InputStream into ActiveMQ message
            msg.setBodyInputStream(targetStream);
        } catch(Exception e) {
            System.out.println(e);
        }
        this.messageList.add(msg);

        /* ############################Replaced with new approach######################
        // ############################################################################
        // Create Operator Object
        ActiveMQMessageProducerClient empty_producerClient = new ActiveMQMessageProducerClient();
        OperatorID temp_Operator_ID = new OperatorID(input_IT.getOperator_ID());

        Operator temp_Operator = null;
        if (input_IT.getOperator_Type().equals("OperatorA")){
            OperatorTypeA OperatorA = new OperatorTypeA(empty_producerClient, temp_Operator_ID, input_IT.getOperator_Input_Type());
            temp_Operator = OperatorA;
        }else if (input_IT.getOperator_Type().equals("OperatorB")){
            OperatorTypeB OperatorB = new OperatorTypeB(empty_producerClient, temp_Operator_ID, input_IT.getOperator_Input_Type());
            temp_Operator = OperatorB;
        }else{
            Operator default_Operator = new Operator(empty_producerClient, temp_Operator_ID, input_IT.getOperator_Input_Type());
            temp_Operator = default_Operator;
            System.out.println("Error: Operator Type not found (default operator used)");
        }


        // Load Operator
        try {
            // Create ByteArrayOutputStream
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(temp_Operator);
            oos.flush();
            oos.close();
            InputStream targetStream = new ByteArrayInputStream(baos.toByteArray());

            // Put InputStream into ActiveMQ message
            msg.setBodyInputStream(targetStream);
            // Put ActiveMQ message to ready-to-send list
            this.messageList.add(msg);

        } catch(Exception e) {
            System.out.println(e);
        }
        // ###########################################################################
        */





    }



    // create a queue for each element in queue_name with its filter (same name)
    public void createQueue() {
        for (Iterator<String> i = queue_names.iterator(); i.hasNext();){
            String temp_queue_name = i.next();
            try{
                String temp_filter = "Task_Manager_ID = '"+temp_queue_name+"'";
                // Create Multicast(Pub-Sub) queue
                session.createQueue(this.address_name, RoutingType.MULTICAST, temp_queue_name, temp_filter, true);
                System.out.println("Created queue: " + temp_queue_name);
            } catch(Exception e) {
                System.out.println(e);
            }// end try-catch

        }// end for-loop
    }



    // send all messages in the messageList
    public void sendAll()   {
        for (Iterator<ClientMessage> i = this.messageList.iterator(); i.hasNext();) {
            try{
                ClientMessage temp_message = i.next();
//                System.out.println(ReflectionToStringBuilder.toString(temp_message));
                this.producer.send(temp_message);
            }catch (ActiveMQException e){
                System.out.println(e);
            }

        }
    }

    // send all messages in the messageList
    public void sendAllwithSleep(int sleepTime)   {
        for (Iterator<ClientMessage> i = this.messageList.iterator(); i.hasNext();) {
            try{
                ClientMessage temp_message = i.next();
                this.producer.send(temp_message);
                this.session.commit();
                Thread.sleep(sleepTime);
            }catch (ActiveMQException | InterruptedException e){
                System.out.println(e);
            }

        }
    }

    public void begin(){
        try{
            this.session.start();
        }catch (ActiveMQException e){
            System.out.println(e);
        }
    }

    public void commit(){
        try{
            this.session.commit();
        }catch (ActiveMQException e){
            System.out.println(e);
        }

    }

    public void end(){
        try{
            this.session.close();
        }catch (ActiveMQException e){
            System.out.println(e);
        }
    }

    public void createConsumer(String my_queue_name, String test_filter) throws ActiveMQException {
        this.consumer = this.session.createConsumer(my_queue_name, test_filter);
    }

    /*
    public String getMsg() throws ActiveMQException, IOException {
        ClientMessage msgReceived = this.consumer.receive();
        //msgReceived.acknowledge();

        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        try {
            msgReceived.saveToOutputStream(bos);
        } catch (ActiveMQException e) {
            e.printStackTrace();
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        ObjectInputStream in = null;
        in = new ObjectInputStream(bis);
        Object o = in.readObject();



        return result.toString();
    }

     */

}
