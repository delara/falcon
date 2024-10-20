package com.edgestream.worker.runtime.plan;

import java.util.ArrayList;
import java.util.List;

public class Physical_Plan {
    private final String Topology_ID;
    private final List<Individual_Plan> Individual_Plans;

    public Physical_Plan(String topology_ID) {
        Topology_ID = topology_ID;
        Individual_Plans = new ArrayList<Individual_Plan>();
    }


    public List<Individual_Plan> getIndividual_Plans() {
        return Individual_Plans;
    }

    public void addIndividual_Plans(Individual_Plan new_Individual_Plan) {
        Individual_Plans.add(new_Individual_Plan);
    }




}
