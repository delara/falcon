package com.edgestream.worker.io.artemis;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.*;


public class ActiveMQOPtoOperatorManagementProducer {

    private final String managementFQQN;
    private ServerLocator locator;
    private ClientSession session;
    private ClientProducer producer;

    public ActiveMQOPtoOperatorManagementProducer(String managementFQQN, String brokerIP) {

        this.managementFQQN = managementFQQN;
        try {
            this.locator = ActiveMQClient.createServerLocator(brokerIP);
            ClientSessionFactory factory = locator.createSessionFactory();
            this.session = factory.createSession();
            System.out.println("[ActiveMQOPtoOperatorManagementProducer]: Connected to [OP] -> [OP] Queue on Artemis Broker");

            this.session.start();
            this.producer = this.session.createProducer(this.managementFQQN);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    public void sendSwitchPredecessorOutputToZMQMessage(String reconfigurationPlanID, String sourceOperatorID, String destinationOperatorID, String predecessorIP){

        ClientMessage msg_to_send = this.session.createMessage(true);

        long timeStamp = System.currentTimeMillis();

        msg_to_send.setTimestamp(timeStamp);
        String timeStampString = String.valueOf(timeStamp);
        msg_to_send.putStringProperty("timeStamp", timeStampString);
        msg_to_send.putStringProperty("messageType", "switch_output");
        msg_to_send.putStringProperty("reconfigurationPlanID", reconfigurationPlanID);
        msg_to_send.putStringProperty("source_operatorID", sourceOperatorID);
        msg_to_send.putStringProperty("destination_operatorID", destinationOperatorID);
        msg_to_send.putStringProperty("predecessorIP", predecessorIP);

        try {
            this.producer.send(msg_to_send);
            this.session.commit();
            System.out.println("[ActiveMQOPtoOperatorManagementProducer]: Output switch request [AMQ to ZMQ] sent to operatorID [" + destinationOperatorID + "]" + " predecessorIP ["+  predecessorIP +"]");
        } catch (ActiveMQException e) {
            e.printStackTrace();
        }

    }



    public void sendReadyMessageToPredecessor(String reconfigurationPlanID, String sourceOperatorID, String destinationOperatorID, String predecessorIP){

        ClientMessage msg_to_send = this.session.createMessage(true);

        long timeStamp = System.currentTimeMillis();

        msg_to_send.setTimestamp(timeStamp);
        String timeStampString = String.valueOf(timeStamp);
        msg_to_send.putStringProperty("timeStamp", timeStampString);
        msg_to_send.putStringProperty("messageType", "successor_ready");
        msg_to_send.putStringProperty("reconfigurationPlanID", reconfigurationPlanID);
        msg_to_send.putStringProperty("source_operatorID", sourceOperatorID);
        msg_to_send.putStringProperty("destination_operatorID", destinationOperatorID);
        msg_to_send.putStringProperty("predecessorIP", predecessorIP);

        try {
            this.producer.send(msg_to_send);
            this.session.commit();
            System.out.println("[ActiveMQOPtoOperatorManagementProducer]: Successor ready message sent to container IP: " + predecessorIP);
        } catch (ActiveMQException e) {
            e.printStackTrace();
        }

    }
}
