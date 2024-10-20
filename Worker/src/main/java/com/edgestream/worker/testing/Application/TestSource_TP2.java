package com.edgestream.worker.testing.Application;

import com.edgestream.worker.common.Tuple;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.*;

import java.io.*;

public class TestSource_TP2 {



    public static void main(final String[] args) throws Exception {


        String broker_ip = "10.70.20.48";


        ServerLocator  locator  = ActiveMQClient.createServerLocator("tcp://"+ broker_ip + ":61616");

        locator.setConfirmationWindowSize(3000000);


        ClientSessionFactory factory =  locator.createSessionFactory();
        //ClientSession session = factory.createTransactedSession();
        ClientSession session = factory.createSession();
        session.setSendAcknowledgementHandler(new MySendAcknowledgementsHandler());



        //String address = "primary_topology_pipeline_test_tp_1";
        //String address = "merlin_default";

        // producer = session.createProducer(address);
        ClientProducer producer = session.createProducer();

        int numberToSend = 10;

        //1. create the test Tuple, set the filter to A
        //2. covert to bytes
        //3. send the tuple
        System.out.println("Sending " + numberToSend + " Tuple(s)");

        int counter = 1;

        for (int i =0 ; i < numberToSend;i++ ) {

            System.out.println("Sending a tuple");

            Tuple tupleToSend = new Tuple();
            tupleToSend.setPayload("Test Source TP_002: A tuple from Test Source");
            tupleToSend.setType("R");
            //MessageProducerClient messageProducerClient = new ActiveMQMessageProducerClient();
            //OperatorID operatorID = new OperatorID("Operator 1");
            //Operator operator = new Operator(messageProducerClient, operatorID,"type A");


            //convert Tuple to byte array
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out;
            byte[] yourBytes;
            try {
                out = new ObjectOutputStream(bos);
                out.writeObject(tupleToSend);
                out.flush();
                yourBytes = bos.toByteArray();

            } finally {
                try {
                    bos.close();
                } catch (IOException ex) {
                    // ignore close exception
                }
            }

            //attached the byte array to an input stream
            ByteArrayInputStream bis = new ByteArrayInputStream(yourBytes);


            //1. create the message object
            ClientMessage msg_to_send = session.createMessage(true);
            msg_to_send.setBodyInputStream(bis);
            bis.close();


            //2. set the timestamp
            msg_to_send.setTimestamp(System.currentTimeMillis());
            //3. set the tuple type (as an attribute)


            msg_to_send.putStringProperty("tupleType", tupleToSend.getType());
            msg_to_send.putStringProperty("topologyID","tpstatsv3001");
            msg_to_send.putStringProperty("sensorid_type","ci4wg4xti000502tccs34dvk421humidity");
            //msg_to_send.putStringProperty("tupleType", tupleToSend.getType());

            //5. send the message to the queue(this will read the byte stream and put it into the ActiveMQ msg)


            //msg_to_send.setAddress("tptest001_typeF");




            producer.send("merlin_default",msg_to_send);
            //System.out.println("Tuple sent");

            counter ++;

            if (counter >= numberToSend){

                session.commit();
                counter = 0;
                System.out.println("Batch Sent...");


            }







        }
        System.out.println("Connection closed...");
        session.close();


    }

    static class MySendAcknowledgementsHandler implements SendAcknowledgementHandler {

        int count = 0;

        @Override
        public void sendAcknowledged(final Message message) {

           //System.out.println("Received send acknowledgement for message " + count++);

        }
    }


}
