package com.edgestream.clustermanager.scheduler;

import com.edgestream.clustermanager.plan.ConvertToPhysicalPlan;
import com.edgestream.worker.runtime.plan.*;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;


public class JobSchedulerManager {
    private String ppo_path;
    private String pph_path;
    private Physical_Plan pp;

    private Physical_Plan_H ppH_Object;
    private Physical_Plan_O ppO_Object;

    private ArrayList<Individual_Plan> Individual_Plans;
    private ArrayList<Individual_Task> Individual_Tasks;

    public JobSchedulerManager() {
    }

    public JobSchedulerManager(String ppo_path, String pph_path) {
        this.ppo_path = ppo_path;
        this.pph_path = pph_path;
    }


    /**
     * An alternative constructor that can take objects instead of JSON files
     * */
    public JobSchedulerManager(Physical_Plan_O ppO_Object, Physical_Plan_H ppH_Object) {
        this.ppO_Object = ppO_Object;
        this.ppH_Object = ppH_Object;

    }


    /**
     * Does the same job as {@link #physicalPlanGenerator()} but takes objects instead
     * */
    public Physical_Plan generatePhysicalPlan(){

        ConvertToPhysicalPlan cpp = new ConvertToPhysicalPlan(this.ppO_Object,this.ppH_Object);
        this.pp = cpp.getPhysicalPLan();

        return pp;
    }


    /**
     * This is used when you submit a JSON file
     * No longer used
     * */
    @Deprecated
    public void physicalPlanGenerator() throws FileNotFoundException {
        this.pp  = (Physical_Plan) new ConvertToPhysicalPlan(ppo_path,pph_path).JSONtoObject();
    }



    public void taskGenerator(long Job_Submission_Time){

        ArrayList<Individual_Plan> Individual_Plans = (ArrayList<Individual_Plan>) this.pp.getIndividual_Plans();
        // create new Individual_Task
        this.Individual_Tasks = new ArrayList<Individual_Task>();
        int task_id = 0;
        for (Iterator<Individual_Plan> i = Individual_Plans.iterator(); i.hasNext();) {
            Individual_Plan temp_Individual_Plan = i.next();
            for (Iterator<Operator_Src> j = temp_Individual_Plan.getOperator_Srcs().iterator(); j.hasNext();){

                Operator_Src temp_Operator_Src = j.next();

                Individual_Task temp_Individual_Task = new Individual_Task(temp_Individual_Plan.getTopology_ID(),
                        Integer.toString(task_id),
                        temp_Individual_Plan.getTask_Manager_ID(), temp_Individual_Plan.getHost_Name(),
                        temp_Operator_Src.getOperator_ID(), temp_Operator_Src.getOperator_Type(),
                        temp_Operator_Src.getOperator_Input_Type(), temp_Operator_Src.getOperator_Path(),
                        temp_Operator_Src.getOperator_Mode(), temp_Individual_Plan.getHost_Address(),
                        temp_Individual_Plan.getHost_Port(), temp_Operator_Src.getMessage_Routing_Type(),
                        temp_Operator_Src.getPredecessor_IDs(), temp_Operator_Src.getSuccessor_IDs(),
                        temp_Operator_Src.getDagElementType(),
                        Job_Submission_Time);

                if(temp_Operator_Src.hasBootstrapState()){

                    temp_Individual_Task.addStateTransferPlan(temp_Operator_Src.getStateTransferPlan());

                }

                Individual_Tasks.add(temp_Individual_Task);
            }// end for-loop
        }// end for-loop
    }

    public ArrayList<Individual_Task> getIndividual_Tasks() {
        return Individual_Tasks;
    }




}
