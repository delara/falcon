package com.edgestream.worker.runtime.topology;


import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.ActiveMQObjectClosedException;
import org.apache.activemq.artemis.api.core.client.*;

public class TopologyManagementListener extends Thread {

    private ClientConsumer consumer;
    private ClientSession session;
    private final Topology boundTopology;
    private final String topologyManagementFQQN;
    private final String taskQueueBrokerIP;

    public TopologyManagementListener(String topologyManagementFQQN, String taskQueueBrokerIP, Topology topology) {
        this.topologyManagementFQQN = topologyManagementFQQN;
        this.taskQueueBrokerIP =taskQueueBrokerIP;
        this.boundTopology = topology;
        connectToBroker(topologyManagementFQQN, taskQueueBrokerIP);
    }



    private void connectToBroker(String topologyManagementFQQN, String localBrokerIP) {

        try {
            System.out.println("[TopologyManagementListener] :" + "Attempting to connect to the Broker at IP: " + localBrokerIP);
            ServerLocator locator = ActiveMQClient.createServerLocator("tcp://" + localBrokerIP + ":61616");

            ClientSessionFactory factory = locator.createSessionFactory();
            session = factory.createSession();
            consumer = session.createConsumer(topologyManagementFQQN);
            session.start();

        } catch (ActiveMQNonExistentQueueException e) {
            System.out.println("Local broker and management queue does not yet exist, please check to see if the broker is started...");
            e.printStackTrace();



        } catch (Exception e) {

            e.printStackTrace();
        }

    }


    @Override
    public void run() {

        while(!session.isClosed()) { //loop indefinitely waiting for new management messages from the operators
            try {

                System.out.println("[TopologyManagementListener]: --------------------" + "Topology Management Listener ready and waiting for a message from the operators.......");
                ClientMessage msgReceived = consumer.receive(); // This will block until a message is received
                msgReceived.acknowledge();
                this.session.commit();


                if (msgReceived.getStringProperty("messageType").equalsIgnoreCase("setup")) {
                    if (msgReceived.getStringProperty("SetupStatus").equalsIgnoreCase("completed")) {

                        String reconfigurationPlanID = msgReceived.getStringProperty("reconfigurationPlanID");
                        String operatorID = msgReceived.getStringProperty("operatorID");
                        //System.out.println("Operator " + operatorID + " container is online and ready to accept tuples, will dequeue from confirmation queue");

                        boundTopology.getTopologyReconfigurationManager().dequeOperatorFromConfirmationQueue(reconfigurationPlanID, operatorID);
                    }
                }

            } catch (ActiveMQObjectClosedException e) {
                System.out.println("[TopologyManagementListener]: Local broker is offline, check to see if broker has been started, exiting...");

            } catch (ActiveMQException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }//end of while loop

        System.out.println("[TopologyManagementListener]: Something has gone wrong the session was not able to open, check broker configs!"); // THIS LINE SHOULD NEVER BE REACHED
        printConfig();

    }

    private void printConfig(){
        System.out.println("[TopologyManagementListener]: Here is the current config: ");
        System.out.println("[TopologyManagementListener]: Broker IP: [" + this.taskQueueBrokerIP +"]");
        System.out.println("[TopologyManagementListener]: Management FQQN: [" + this.topologyManagementFQQN+"]");
    }

}
