package com.edgestream.clustermanager.manager;

import com.edgestream.clustermanager.scheduler.JobSchedulerManager;
import com.edgestream.worker.runtime.plan.Individual_Task;
import com.edgestream.clustermanager.broker.TQmanager;
import com.edgestream.worker.runtime.docker.DockerFileType;


import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;

public class mainClusterManagerCMD {


    public static void main(String[] args) throws FileNotFoundException {

        System.out.println("Cluster Manager Begins:");


        String server_address = args[0];
        System.out.println("Server Address: " + server_address);
        String ppo_path = args[1];
        System.out.println("Physical_Plan_O path: " + ppo_path);
        String pph_path = args[2];
        System.out.println("Physical_Plan_H path: " + pph_path);


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

        Task_Queue.createQueue();
        System.out.println("createQueue() done!");

        Task_Queue.sendAll();
        System.out.println("sendAll() done!");


        Task_Queue.begin();
        System.out.println("start session done!");

        Task_Queue.commit();
        System.out.println("commit session done!");

        Task_Queue.end();
        System.out.println("close session done!");


    }
}
