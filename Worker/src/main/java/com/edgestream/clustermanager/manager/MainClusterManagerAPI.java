package com.edgestream.clustermanager.manager;

import com.edgestream.clustermanager.model.Job;
import com.edgestream.clustermanager.scheduler.JobSchedulerManager;
import com.edgestream.clustermanager.broker.TQmanager;
import com.edgestream.worker.runtime.plan.Individual_Task;
import com.edgestream.worker.runtime.plan.Physical_Plan;
import com.edgestream.worker.runtime.plan.Physical_Plan_H;
import com.edgestream.worker.runtime.plan.Physical_Plan_O;
import com.edgestream.worker.runtime.docker.DockerFileType;
import com.edgestream.worker.runtime.reconfiguration.common.DagPlan;
import com.edgestream.worker.runtime.reconfiguration.execution.ExecutionPlan;
import com.edgestream.worker.runtime.reconfiguration.execution.ExecutionType;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;

public class MainClusterManagerAPI {

    ArrayList<Job> managedJobs = new ArrayList<>();

    public MainClusterManagerAPI() {

        System.out.println("[MainClusterManagerAPI]: Cluster Manager Begins:");

    }


    public void submitJob(String taskBrokerAddress, ExecutionPlan executionPlan){

        String JobID = executionPlan.getReconfigurationPlan().getReconfigurationPlanID();
        Physical_Plan_O ppo = executionPlan.getReconfigurationPlan().getPhysical_plan_o();
        Physical_Plan_H pph = executionPlan.getReconfigurationPlan().getPhysical_plan_h();
        DockerFileType dockerFileType = executionPlan.getDockerFileType();
        ArrayList<ImmutablePair> dag = executionPlan.getDag();
        DagPlan dagPlan = executionPlan.getDagPlan();
        String reconfigurationPlanID = executionPlan.getReconfigurationPlan().getReconfigurationPlanID();
        ExecutionType executionType = executionPlan.getExecutionType();



        System.out.println("[MainClusterManagerAPI]: Received new job to submit: " + JobID);

        long Job_Submission_Time = System.currentTimeMillis();
        System.out.print("[MainClusterManagerAPI]: Begin Time : ");
        System.out.println(Job_Submission_Time);


        System.out.println("[MainClusterManagerAPI]: Reading Physical Plan...");
        JobSchedulerManager JS = new JobSchedulerManager(ppo,pph);
        Physical_Plan managed_pp = JS.generatePhysicalPlan();


        System.out.println("[MainClusterManagerAPI]: Physical Plan done!");

        JS.taskGenerator(Job_Submission_Time);
        ArrayList<Individual_Task> Individual_Tasks = JS.getIndividual_Tasks();



        TQmanager tQmanager = new TQmanager(taskBrokerAddress);
        tQmanager.createProducer("pending_tasks");

        for (Iterator<Individual_Task> i = Individual_Tasks.iterator(); i.hasNext();) {
            Individual_Task temp_Individual_Task = i.next();
            try {
                tQmanager.messageBuilder(temp_Individual_Task , Individual_Tasks,dockerFileType ,dag,dagPlan, reconfigurationPlanID,executionType);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        System.out.println("[MainClusterManagerAPI]: All messages have been built");

        tQmanager.createQueue();
        System.out.println("[MainClusterManagerAPI]:  createQueue() done!");

        System.out.println(" ");
        System.out.println("[MainClusterManagerAPI]: <<  IN PROGRESS  >> Uploading operator jars to cluster manager <<  IN PROGRESS  >>");
        System.out.println(" ");


//        tQmanager.sendAllwithSleep(300);
//        System.out.println("Upload completed!");

        tQmanager.sendAll();
        System.out.println("[MainClusterManagerAPI]: Upload completed!");


        tQmanager.begin();
        System.out.println("[MainClusterManagerAPI]: start session done!");

        tQmanager.commit();
        System.out.println("[MainClusterManagerAPI]: commit session done!");
//
        tQmanager.end();
        System.out.println("[MainClusterManagerAPI]: close session done!");



        Job job = new Job(JobID,taskBrokerAddress,managed_pp);

        managedJobs.add(job);

    }

}
