package com.edgestream.clustermanager.plan;


import com.edgestream.worker.runtime.plan.*;
import com.edgestream.worker.runtime.reconfiguration.common.DagElementType;
import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Iterator;


public class ConvertToPhysicalPlan {
    private String JSONfile_O_path;
    private String JSONfile_H_path;
    private Physical_Plan_O ppo;
    private Physical_Plan_H pph;

    // test
    public ConvertToPhysicalPlan() {
        this.JSONfile_O_path = "com.edgestream.clustermanager.samplePlan/physical_plan_sample_O.json";
        this.JSONfile_H_path = "com.edgestream.clustermanager.samplePlan/physical_plan_sample_H.json";
    }

    // regular
    public ConvertToPhysicalPlan(String JSONfile_O_path, String JSONfile_H_path) {
        this.JSONfile_O_path = JSONfile_O_path;
        this.JSONfile_H_path = JSONfile_H_path;
    }

    /**
     * A constructor that takes objects, JSON files
     * */
    public ConvertToPhysicalPlan(Physical_Plan_O ppo, Physical_Plan_H pph) {

        this.ppo = ppo;
        this.pph = pph;


    }

    public Physical_Plan getPhysicalPLan(){

        // read two JSON files and convert each of them into an object
        //BufferedReader JSONfile_O = new BufferedReader(new FileReader(JSONfile_O_path));
        //BufferedReader JSONfile_H = new BufferedReader(new FileReader(JSONfile_H_path));
        Physical_Plan_O PPO = this.ppo;
        Physical_Plan_H PPH = this.pph;

        // Future work: Check for Topology ID consistency
        //
        Physical_Plan result = new Physical_Plan(PPO.getTopology_ID());


        // create new Individual_Plan for each host from PPH
        for (Iterator<Host> i = PPH.getHosts().iterator(); i.hasNext();) {
            Host temp_Host = i.next();
            Individual_Plan temp_Individual_Plan = new Individual_Plan(PPH.getTopology_ID(), temp_Host.getName(),
                    temp_Host.getTask_Manager_ID(), temp_Host.getHost_Address(), temp_Host.getHost_Port());
            // add it to Physical_Plan
            result.addIndividual_Plans(temp_Individual_Plan);
        }// end for-loop

        // create new Operator_Src for each operator from PPO
        for (Iterator<OperatorInPPO> j = PPO.getOperators().iterator(); j.hasNext();) {
            OperatorInPPO temp_Operator = j.next();
            DagElementType dagElementType= DagElementType.STATELESS; // the default
            Operator_Src temp_Operator_Src = new Operator_Src(temp_Operator.getOperator_ID(), temp_Operator.getType(),
                    temp_Operator.getInput_Type(), temp_Operator.getRouting_Type(), temp_Operator.getFilepath(),
                    temp_Operator.getMode(), temp_Operator.getPredecessor_IDs(), temp_Operator.getSuccessor_IDs(),dagElementType);

            if(temp_Operator.isStateful()){
                temp_Operator_Src.addStateTransferPlan(temp_Operator.getStateTransferPlan());
            }


            // for each Individual_Plan inside Physical_Plan, if they are involved, add Operator_Src
            for (Iterator<Individual_Plan> k = result.getIndividual_Plans().iterator(); k.hasNext();) {
                Individual_Plan temp_Individual_Plan = k.next();
                if (temp_Operator.getHost_names().contains(temp_Individual_Plan.getHost_Name())){
                    temp_Individual_Plan.addOperator_Srcs(temp_Operator_Src);
                }
            }// end for-loop
        }// end for-loop
        return result;

    }

    public Object JSONtoObject() throws FileNotFoundException {
        // read two JSON files and convert each of them into an object
        BufferedReader JSONfile_O = new BufferedReader(new FileReader(JSONfile_O_path));
        BufferedReader JSONfile_H = new BufferedReader(new FileReader(JSONfile_H_path));
        Physical_Plan_O PPO = new Gson().fromJson(JSONfile_O, Physical_Plan_O.class);
        Physical_Plan_H PPH = new Gson().fromJson(JSONfile_H, Physical_Plan_H.class);

        // Future work: Check for Topology ID consistency
        //
        Physical_Plan result = new Physical_Plan(PPO.getTopology_ID());


        // create new Individual_Plan for each host from PPH
        for (Iterator<Host> i = PPH.getHosts().iterator(); i.hasNext();) {
            Host temp_Host = i.next();
            Individual_Plan temp_Individual_Plan = new Individual_Plan(PPH.getTopology_ID(), temp_Host.getName(),
                    temp_Host.getTask_Manager_ID(), temp_Host.getHost_Address(), temp_Host.getHost_Port());
            // add it to Physical_Plan
            result.addIndividual_Plans(temp_Individual_Plan);
        }// end for-loop

        // create new Operator_Src for each operator from PPO
        for (Iterator<OperatorInPPO> j = PPO.getOperators().iterator(); j.hasNext();) {
            OperatorInPPO temp_Operator = j.next();
            DagElementType dagElementType= DagElementType.STATELESS; // the default
            Operator_Src temp_Operator_Src = new Operator_Src(temp_Operator.getOperator_ID(), temp_Operator.getType(),
                    temp_Operator.getInput_Type(), temp_Operator.getRouting_Type(), temp_Operator.getFilepath(),
                    temp_Operator.getMode(), temp_Operator.getPredecessor_IDs(), temp_Operator.getSuccessor_IDs(),dagElementType);

            if(temp_Operator.isStateful()){
                temp_Operator_Src.addStateTransferPlan(temp_Operator.getStateTransferPlan());
            }

            // for each Individual_Plan inside Physical_Plan, if they are involved, add Operator_Src
            for (Iterator<Individual_Plan> k = result.getIndividual_Plans().iterator(); k.hasNext();) {
                Individual_Plan temp_Individual_Plan = k.next();
                if (temp_Operator.getHost_names().contains(temp_Individual_Plan.getHost_Name())){
                    temp_Individual_Plan.addOperator_Srcs(temp_Operator_Src);
                }
            }// end for-loop
        }// end for-loop
        return result;

    }// end JSONtoObject method





}
