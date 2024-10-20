package com.edgestream.worker.runtime.reconfiguration;

import com.edgestream.worker.runtime.plan.Host;
import com.edgestream.worker.runtime.plan.OperatorInPPO;
import com.edgestream.worker.runtime.plan.Physical_Plan_H;
import com.edgestream.worker.runtime.plan.Physical_Plan_O;
import com.edgestream.worker.runtime.docker.DockerFileType;
import com.google.gson.Gson;
import org.apache.commons.lang3.tuple.ImmutablePair;
import java.util.ArrayList;
import java.util.List;


public class ReconfigurationPlan {


    private final String reconfigurationPlanID;
    private final String topologyID;
    private final List<Host> destinationDataCenterTaskManagers;
    private final List<OperatorInPPO> OperatorsInPPO;
    private String H_plan_as_JSON;
    private String O_plan_as_JSON;
    private Physical_Plan_H physical_plan_h;
    private Physical_Plan_O physical_plan_o;
    private final DockerFileType dockerFileType;
    ArrayList<ImmutablePair> dag;

    public ReconfigurationPlan(String reconfigurationPlanID, String topologyID, List<Host> destinationDataCenterTaskManagers, List<OperatorInPPO> OperatorsInPPO, DockerFileType dockerFileType, ArrayList<ImmutablePair> dag) {

        this.reconfigurationPlanID = reconfigurationPlanID;
        this.topologyID = topologyID;
        this.destinationDataCenterTaskManagers = destinationDataCenterTaskManagers;
        this.OperatorsInPPO = OperatorsInPPO;
        this.dockerFileType = dockerFileType;
        this.dag = dag;

        createAndSetHPLan();
        createAndSetOPLan();

    }

    public ArrayList<ImmutablePair> getDag() {
        return dag;
    }

    public void createAndSetHPLan(){

        String planType = "Hosts";
        this.physical_plan_h = new Physical_Plan_H(this.topologyID,planType,this.destinationDataCenterTaskManagers);
        //this.H_plan_as_JSON =  new Gson().toJson(this.physical_plan_h);

    }

    public void createAndSetOPLan(){

        String planType = "Operators";
        this.physical_plan_o = new Physical_Plan_O(this.topologyID, planType, this.OperatorsInPPO);
        //this.O_plan_as_JSON = new Gson().toJson(this.physical_plan_o);

    }


    public String getReconfigurationPlanID() {
        return reconfigurationPlanID;
    }

    public String getTopologyID() {
        return topologyID;
    }


    private String getH_plan_as_JSON() {
        return H_plan_as_JSON;
    }

    private String getO_plan_as_JSON() {
        return O_plan_as_JSON;
    }


    public Physical_Plan_H getPhysical_plan_h() {
        return physical_plan_h;
    }

    public Physical_Plan_O getPhysical_plan_o() {
        return physical_plan_o;
    }



}
