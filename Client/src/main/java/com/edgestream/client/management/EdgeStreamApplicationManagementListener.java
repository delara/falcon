package com.edgestream.client.management;

import com.edgestream.client.EdgeStreamApplicationContext;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.ActiveMQObjectClosedException;
import org.apache.activemq.artemis.api.core.client.*;



public class EdgeStreamApplicationManagementListener extends Thread{

    private ClientConsumer consumer;
    private ClientSession session;
    private final EdgeStreamApplicationContext edgeStreamApplicationContext;

    public EdgeStreamApplicationManagementListener(String OPtoOPManagementFQQN, String localDatacenterBrokerIP , EdgeStreamApplicationContext edgeStreamApplicationContext){

        this.edgeStreamApplicationContext = edgeStreamApplicationContext;
        connectToBroker(OPtoOPManagementFQQN, localDatacenterBrokerIP);
    }

    private void connectToBroker(String OPtoOPManagementFQQN, String localDatacenterBrokerIP) {

        try{
            ServerLocator locator = ActiveMQClient.createServerLocator(localDatacenterBrokerIP);
            ClientSessionFactory factory = locator.createSessionFactory();
            session = factory.createSession();
            consumer = session.createConsumer(OPtoOPManagementFQQN,"(destination_operatorID='"+edgeStreamApplicationContext.getOperatorID()+"')");
            session.start();
            System.out.println("[EdgeStreamApplicationManagementListener]: --------------------" + "Operator Management Listener Connected to Broker at IP: " + localDatacenterBrokerIP);
        } catch (ActiveMQNonExistentQueueException e) {
            e.printStackTrace();
            System.out.println("[EdgeStreamApplicationManagementListener]: Local broker and management queue does not yet exist, please check to see if the broker is started...");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void run() {
        System.out.println("[EdgeStreamApplicationManagementListener]: [OP] to [OP] Management Listener ready and waiting for another message from the operators.......");
        while(true) { //loop indefinitely waiting for new management messages from the operators
            try {
                ClientMessage msgReceived = consumer.receive(); // This will block until a message is received
                System.out.println("[EdgeStreamApplicationManagementListener]: I received an message from another operator!.......");
                msgReceived.acknowledge();
                session.commit();

                String local_operatorID = edgeStreamApplicationContext.getOperatorID();
                String destination_operatorID = msgReceived.getStringProperty("destination_operatorID");
                String messageType = msgReceived.getStringProperty("messageType");

                String reconfigurationPlanID = msgReceived.getStringProperty("reconfigurationPlanID");
                String source_operatorID = msgReceived.getStringProperty("source_operatorID");
                String containerToSwitchIP = msgReceived.getStringProperty("predecessorIP");

                if (messageType.equalsIgnoreCase("switch_output") && destination_operatorID.equalsIgnoreCase(local_operatorID)) {
                    //call the switch the producer method
                    System.out.println("[EdgeStreamApplicationManagementListener]: Received a message to switch to ZMQ from ["+ source_operatorID +"]");

                    edgeStreamApplicationContext.switchOutputToZMQ("FUSED");

                }else if (messageType.equalsIgnoreCase("successor_ready") && destination_operatorID.equalsIgnoreCase(local_operatorID)) {
                    //TODO: need to make this smarter, currently one successor can trigger a start to the context, also if this operator was already running this should be ignored or trigger something else.
                    // Likely we need a queue here and each new operator needs to send predecessors announcement tuples.

                    edgeStreamApplicationContext.startContext();
                    System.out.println("[EdgeStreamApplicationManagementListener]: Received a successor_ready message from  ["+ source_operatorID +"]");

                }else{
                    if(destination_operatorID.equalsIgnoreCase(local_operatorID)){
                        System.out.println("[EdgeStreamApplicationManagementListener]: I got a message and its for me but it don't know what to do with it. The message is from [" + source_operatorID + "] and the type is: [" +  messageType + "]" );
                    }else{
                        System.out.println("[EdgeStreamApplicationManagementListener]: This message is NOT for me so I am ignoring it. The message is from [" + source_operatorID + "] and the type is: [" +  messageType + "]" );
                    }
                }
            } catch (ActiveMQObjectClosedException e) {
                System.out.println("[EdgeStreamApplicationManagementListener]: Local broker is offline, check to see if broker has been started, exiting...");
            } catch (ActiveMQException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}
