package com.edgestream.worker.io.artemis;

import com.edgestream.worker.metrics.exception.EdgeStreamMetricsMessageException;
import com.edgestream.worker.metrics.model.MetricType;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.*;

import java.io.FileNotFoundException;



public class ActiveMQMessageLocalMetricsConsumer {


    /*****************************************************************
     * A consumer that reads metrics from the topology local metrics queue
     *
     *****************************************************************/

    private final String localMetricsFQQN;
    private final ArtemisMetricsConsumer primaryConsumer;


    public ActiveMQMessageLocalMetricsConsumer(String localMetricsFQQN, String brokerIP) throws Exception {

        this.localMetricsFQQN = localMetricsFQQN;
        this.primaryConsumer = new ArtemisMetricsConsumer(this.localMetricsFQQN, brokerIP);


    }


    public void startLocalMetricsConsumerTask() {

        this.primaryConsumer.start();
    }


    class ArtemisMetricsConsumer extends Thread {
        ClientConsumer consumer1;
        ClientSession session;
        Thread publish;


        public ArtemisMetricsConsumer(String localMetricsFQQN, String brokerIP) throws Exception {

            System.out.println("Starting local metrics consumer");
            ServerLocator locator = ActiveMQClient.createServerLocator(brokerIP);
            ClientSessionFactory factory = locator.createSessionFactory();
            session = factory.createSession();


            try {
                session.start();
            } catch (ActiveMQException e) {
                e.printStackTrace();
            }


            System.out.println("Connected to local metrics Broker");


            //try to connect to the local metrics queue
            try {
                consumer1 = session.createConsumer(localMetricsFQQN);
            } catch (ActiveMQException e) {
                e.printStackTrace();
            }
            try {
                consumer1.setMessageHandler(new MyMessageHandler());

            } catch (ActiveMQException e) {
                e.printStackTrace();
            }

        }


        /*****************************
         * MyMessageHandler is invoked when a message arrives. This happens asynchronously. The session is committed by the onMessage() method after the batch size count
         * threshold has been hit
         *******************************/
        class MyMessageHandler implements MessageHandler {


            public MyMessageHandler() throws FileNotFoundException {
                System.out.println("Creating local metrics handler");
                System.out.println("Waiting for first message");
                System.out.println("******************************************************");

            }

            /***
             * This is the method that receives the message.
             * @param clientMessage
             */
            @Override
            public void onMessage(ClientMessage clientMessage) {

                /**1. ack and commit ******/
                try {
                    //we only acknowledge, but not yet commit
                    clientMessage.acknowledge();
                    session.commit();

                } catch (ActiveMQException e) {
                    e.printStackTrace();
                }
                if (clientMessage.getStringProperty("message_type").equalsIgnoreCase(MetricType.APPLICATION.toString())) {
                    System.out.println("I received a application metric..");
                    processMetricMessage(clientMessage);
                }else{

                    throw new EdgeStreamMetricsMessageException("Found a message that is not an ApplicationMetric in the metrics queue, something has gone wrong, investigate queue filter",new Throwable());
                }
            }


            private void processMetricMessage(ClientMessage clientMessage){

                //TODO Step1: extract latency data from here

                String transfer = clientMessage.getStringProperty("timestampTransfer");
                String previous_operator = clientMessage.getStringProperty("previousOperator");
                String timeStamp = clientMessage.getStringProperty("timeStamp");


                //TODO Step2: check to see if the transferring time is acceptable

                //TODO Step3: notify operator to detach from warmup queue and connect to live queue

            }
        }//end of MyMessageHandler class
    }//end of ArtemisMetricsConsumer class
}//end of ActiveMQMessageLocalMetricsConsumer

