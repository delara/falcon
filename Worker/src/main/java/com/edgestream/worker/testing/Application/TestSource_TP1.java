package com.edgestream.worker.testing.Application;

import com.edgestream.worker.common.Tuple;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.*;


import java.io.*;

public class TestSource_TP1 {



    public static void main(final String[] args) throws Exception {


        String broker_ip = "192.168.0.179";


        ServerLocator  locator  = ActiveMQClient.createServerLocator("tcp://"+ broker_ip + ":61616");

        locator.setConfirmationWindowSize(3000000);


        ClientSessionFactory factory =  locator.createSessionFactory();
        //ClientSession session = factory.createTransactedSession();
        ClientSession session = factory.createSession();
        session.setSendAcknowledgementHandler(new MySendAcknowledgementsHandler());



        //String address = "primary_topology_pipeline_test_tp_1";
        String address = "primary_topology_pipeline_TP_001";

        ClientProducer producer = session.createProducer(address);

        int numberToSend = 10000;

        //1. create the test Tuple, set the filter to A
        //2. covert to bytes
        //3. send the tuple
        System.out.println("Sending " + numberToSend + " Tuple(s)");

        int counter = 0;

        for (int i =0 ; i < numberToSend;i++ ) {

            System.out.println("Sending a tuple");

            Tuple tupleToSend = new Tuple();
            tupleToSend.setPayload("Test Source TP_001: A tuple from Test Source");
            tupleToSend.setType("A");
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

            //5. send the message to the queue(this will read the byte stream and put it into the ActiveMQ msg)
            producer.send(msg_to_send);
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
