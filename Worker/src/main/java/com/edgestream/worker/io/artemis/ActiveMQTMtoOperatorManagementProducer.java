package com.edgestream.worker.io.artemis;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.*;


public class ActiveMQTMtoOperatorManagementProducer {

    private final String managementFQQN;
    private ServerLocator locator;
    private ClientSession session;
    private ClientProducer producer;

    public ActiveMQTMtoOperatorManagementProducer(String managementFQQN, String brokerIP) {

        this.managementFQQN = managementFQQN;
        try {
            this.locator = ActiveMQClient.createServerLocator(brokerIP);
            ClientSessionFactory factory = locator.createSessionFactory();
            this.session = factory.createSession();
            System.out.println("[ActiveMQTMtoOperatorManagementProducer]: Connected to Management Queue on Artemis Broker");

            this.session.start();
            this.producer = this.session.createProducer(this.managementFQQN);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public void sendReadyStatusMessage(String reconfigurationPlanID, String operatorID){

        ClientMessage msg_to_send = this.session.createMessage(true);

        long timeStamp = System.currentTimeMillis();

        msg_to_send.setTimestamp(timeStamp);
        String timeStampString = String.valueOf(timeStamp);
        msg_to_send.putStringProperty("timeStamp", timeStampString);
        msg_to_send.putStringProperty("messageType", "setup");
        msg_to_send.putStringProperty("reconfigurationPlanID", reconfigurationPlanID);
        msg_to_send.putStringProperty("operatorID", operatorID);
        msg_to_send.putStringProperty("SetupStatus", "completed");

        try {
            this.producer.send(msg_to_send);
            this.session.commit();
            System.out.println("[ActiveMQTMtoOperatorManagementProducer]: Operator ready message has been sent");
        } catch (ActiveMQException e) {
            e.printStackTrace();
        }

    }



}
