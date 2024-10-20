package com.edgestream.clustermanager.manager;

import com.edgestream.clustermanager.scheduler.JobSchedulerManager;
import com.edgestream.worker.runtime.plan.Individual_Task;
import com.edgestream.clustermanager.broker.TQmanager;
import com.edgestream.worker.runtime.docker.DockerFileType;
import org.apache.activemq.artemis.api.core.ActiveMQException;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;

public class mainClusterManager {
    public static void main(String[] args) throws Exception {
        System.out.println("Cluster Manager Begins:");

        while(true) {
            System.out.println("(Y/N):");
            String cmd_input = System.console().readLine();

            if (cmd_input.equals("Y")) {
                runClusterManager();
            } else if (cmd_input.equals("N")) {
                System.out.println("Quit!");
                break;
            } else {
                System.out.println("wrong input");
                continue;
            }
        }

    }

    public static void runClusterManager() throws FileNotFoundException, ActiveMQException {
        System.out.println("Enter Server Address for Artemis Broker:");
        String server_address = System.console().readLine();
        System.out.println("Enter Physical_Plan_O path:");
        String ppo_path = System.console().readLine();
        System.out.println("Enter Physical_Plan_H path:");
        String pph_path = System.console().readLine();


        long Job_Submission_Time = System.currentTimeMillis();
        System.out.print("Begin Time : ");
        System.out.println(Job_Submission_Time);

        System.out.println("Reading Physical Plan...");
        JobSchedulerManager JS = new JobSchedulerManager(ppo_path, pph_path);
        JS.physicalPlanGenerator();
        System.out.println("Physical Plan done!");
        JS.taskGenerator(Job_Submission_Time);
        ArrayList<Individual_Task> Individual_Tasks = JS.getIndividual_Tasks();
        TQmanager Task_Queue = new TQmanager(server_address);
        //TQmanager Task_Queue = new TQmanager("tcp://localhost:61616");
        //TQmanager Task_Queue = new TQmanager("tcp://10.70.20.46:61616");
        Task_Queue.createProducer("pending_tasks");

        for (Iterator<Individual_Task> i = Individual_Tasks.iterator(); i.hasNext();) {
            Individual_Task temp_Individual_Task = i.next();
            Task_Queue.messageBuilder(temp_Individual_Task, Individual_Tasks, DockerFileType.EDGENT, null, null, null, null);
        }
        System.out.println("All messages have been built");

        System.out.println("Enter any input to proceed to create Queue");
        String useless_input_1 = System.console().readLine();
        Task_Queue.createQueue();
        System.out.println("createQueue() done!");

        System.out.println("Enter any input to proceed to send all messages to Queue");
        String useless_input_2 = System.console().readLine();
        Task_Queue.sendAll();
        System.out.println("sendAll() done!");


        System.out.println("Enter any input to proceed to start session");
        String useless_input_3 = System.console().readLine();
        Task_Queue.begin();
        System.out.println("start session done!");

        System.out.println("Enter any input to proceed to commit session");
        String useless_input_4 = System.console().readLine();
        Task_Queue.commit();
        System.out.println("commit session done!");

        System.out.println("Enter any input to proceed to close session");
        String useless_input_5 = System.console().readLine();
        Task_Queue.end();
        System.out.println("close session done!");



        // receive


    }

}
