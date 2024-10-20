package com.edgestream.worker.runtime.plan;

import java.util.List;

public class Physical_Plan_O {
    private final String Topology_ID;
    private String Plan_Type;
    private List<OperatorInPPO> Operators;


    public Physical_Plan_O(String topology_ID, String plan_Type, List<OperatorInPPO> operators) {
        Topology_ID = topology_ID;
        Plan_Type = plan_Type;
        Operators = operators;
    }


    public String getTopology_ID() {
        return Topology_ID;
    }

    public String getPlan_Type() {
        return Plan_Type;
    }

    public void setPlan_Type(String plan_Type) {
        Plan_Type = plan_Type;
    }

    public List<OperatorInPPO> getOperators() {
        return Operators;
    }

    public void setOperators(List<OperatorInPPO> operators) {
        Operators = operators;
    }


}
