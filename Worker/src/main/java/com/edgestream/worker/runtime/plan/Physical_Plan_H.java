package com.edgestream.worker.runtime.plan;

import java.util.List;

public class Physical_Plan_H {

    public Physical_Plan_H(String topology_ID, String plan_Type, List<Host> hosts) {
        Topology_ID = topology_ID;
        Plan_Type = plan_Type;
        Hosts = hosts;
    }

    private final String Topology_ID;
    private final String Plan_Type;
    private final List<Host> Hosts;

    public String getTopology_ID() {
        return Topology_ID;
    }

    public String getPlan_Type() {
        return Plan_Type;
    }

    public List<Host> getHosts() {
        return Hosts;
    }
}
