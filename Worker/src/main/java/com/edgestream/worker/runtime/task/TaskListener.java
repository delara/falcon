package com.edgestream.worker.runtime.task;


import com.edgestream.worker.runtime.task.processor.TaskRequestProcessorContainerRemoval;
import com.edgestream.worker.runtime.task.processor.TaskRequestProcessorContainer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.ActiveMQObjectClosedException;
import org.apache.activemq.artemis.api.core.client.*;



public class TaskListener extends Thread {


    /*****************************************************************
     * A client that is bound to a queue will get tasks and invoke them
     *
     *****************************************************************/


    private ClientConsumer consumer;
    private ClientSession session;
    private TaskDriver taskDriver;




    public TaskListener(String remoteTaskQueueFQQN, String taskQueueBrokerIP, TaskDriver taskDriver) throws Exception {

        connectToClusterManager(remoteTaskQueueFQQN, taskQueueBrokerIP, taskDriver);

    }

    private void connectToClusterManager(String remoteTaskQueueFQQN, String taskQueueBrokerIP, TaskDriver taskDriver) throws InterruptedException {

        try {
            ServerLocator locator = ActiveMQClient.createServerLocator("tcp://" + taskQueueBrokerIP + ":61616");

            ClientSessionFactory factory = locator.createSessionFactory();
            this.session = factory.createSession();

            this.consumer = session.createConsumer(remoteTaskQueueFQQN);

            this.taskDriver = taskDriver;

            session.start();
            System.out.println("[TaskListener] --------------------" + "Connected to remote Task Queue at IP: " + taskQueueBrokerIP);
        } catch (ActiveMQNonExistentQueueException e) {

            e.printStackTrace();
            System.out.println("[TaskListener] Task Manager Queue does not yet exist...");
            System.out.println("[TaskListener] Waiting 20 seconds for cluster manager to come online...");
            Thread.sleep(1000 * 20);

            // Recursively call this function until the remote queue is found and this exception is now longer thrown
            connectToClusterManager(remoteTaskQueueFQQN, taskQueueBrokerIP, taskDriver);

        } catch (Exception e) {

            e.printStackTrace();
        }

    }



    @Override
    public void run() {

        while(true) { //loop indefinitely waiting for new tasks to setup
            try {

                System.out.println("[TaskListener]  --------------------" + "Task Listener ready and waiting for a task request");
                ClientMessage msgReceived = consumer.receive(); // This will block until a task is received
                msgReceived.acknowledge();  // required so that the remote task broker can see that the task was read by this broker
                this.session.commit(); //sends the actual acknowledgement
                long buildStartTime = System.nanoTime();


                /*****************************************************************************************************
                 *
                 *
                 * Deprecated
                 *
                 *
                 *
                 *
                if (msgReceived.getStringProperty("DeploymentMethod").equalsIgnoreCase("instance_internal")) {

                    System.out.println("Received an instance_internal request");
                    TaskRequestProcessorInternalInstance taskRequestProcessorInternalInstance = new TaskRequestProcessorInternalInstance(msgReceived, taskDriver); // when a task is received process it

                    taskRequestProcessorInternalInstance.executeTask();

                }

                if (msgReceived.getStringProperty("DeploymentMethod").equalsIgnoreCase("class_load_internal")) {
                    System.out.println("Received an class_load_internal request");
                    TaskRequestProcessorInternalClassLoader taskRequestProcessorInternalClassLoader = new TaskRequestProcessorInternalClassLoader(msgReceived, taskDriver); // when a task is received process it

                    taskRequestProcessorInternalClassLoader.executeTask();

                }


                if (msgReceived.getStringProperty("DeploymentMethod").equalsIgnoreCase("external")) {
                    //The constructor will start the jar in separate jvm

                    System.out.println("Received an external request");
                    new TaskRequestProcessorExternal(msgReceived, taskDriver); // when a task is received process it

                }
                *********************************************************************************************************/



                if (msgReceived.getStringProperty("DeploymentMethod").equalsIgnoreCase("container")) {
                    //The constructor will start the jar in docker container
                    System.out.println("-----------------------------------------------------------------------------------------------------");
                    System.out.println("[TaskListener]: Received an container request -------------------------------------------------------");
                    System.out.println("-----------------------------------------------------------------------------------------------------");

                    new TaskRequestProcessorContainer(msgReceived, taskDriver); // when a task is received process it

                }


                /*
                if (msgReceived.getStringProperty("DeploymentMethod").equalsIgnoreCase("shutdown")) {
                    //The constructor will start the jar in separate jvm TODO: use a different field than the deployment method to trigger the shutdown

                    System.out.println("Received a shutdown request");
                    new TaskRequestManagement(msgReceived, taskDriver, TaskType.SHUTDOWN); // when a shutdown request process it

                }
                */

                if (msgReceived.getStringProperty("DeploymentMethod").equalsIgnoreCase("remove")) {

                    System.out.println("[TaskListener]: Received a operator removal request");
                    new TaskRequestProcessorContainerRemoval(msgReceived, taskDriver); // when a shutdown request process it

                }


                long timeToBuild = System.nanoTime() - buildStartTime;
                System.out.println(String.format("[TaskListener]: Task setup time (seconds): %o",timeToBuild/1000000000l));


            } catch (ActiveMQObjectClosedException e) {
                System.out.println("[TaskListener]: Cluster manager is offline, sleeping for 30 seconds...");
                try {
                    sleep(1000 * 30);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            } catch (ActiveMQException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}
