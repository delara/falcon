package com.edgestream.worker.runtime.plan;

import java.util.ArrayList;
import java.util.List;

public class Individual_Plan {
    private final String Topology_ID;
    private final String Host_Name;
    private final String Task_Manager_ID;
    private final String Host_Address;
    private final String Host_Port;
    private final List<Operator_Src> Operator_Srcs;

    public Individual_Plan(String topology_ID, String host_Name, String task_Manager_ID, String host_Address, String host_Port) {
        Topology_ID = topology_ID;
        Host_Name = host_Name;
        Task_Manager_ID = task_Manager_ID;
        Host_Address = host_Address;
        Host_Port = host_Port;
        Operator_Srcs = new ArrayList<Operator_Src>();

    }

    public String getTopology_ID() {
        return Topology_ID;
    }

    public String getHost_Name() {
        return Host_Name;
    }

    public String getTask_Manager_ID() {
        return Task_Manager_ID;
    }


    public List<Operator_Src> getOperator_Srcs() {
        return Operator_Srcs;
    }

    public void addOperator_Srcs(Operator_Src new_Operator_Src) {
        Operator_Srcs.add(new_Operator_Src);
    }


    public String getHost_Address() {
        return Host_Address;
    }



    public String getHost_Port() {
        return Host_Port;
    }

}
