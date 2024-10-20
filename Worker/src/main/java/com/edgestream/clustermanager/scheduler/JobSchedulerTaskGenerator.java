package com.edgestream.clustermanager.scheduler;

import com.edgestream.worker.runtime.plan.Individual_Plan;
import com.edgestream.worker.runtime.plan.Physical_Plan;

import java.util.List;

public class JobSchedulerTaskGenerator {
    private static Physical_Plan pp;

    public JobSchedulerTaskGenerator(Physical_Plan pp) {
        JobSchedulerTaskGenerator.pp = pp;
    }

    public List<Individual_Plan> build() {
        return pp.getIndividual_Plans();
    }

}
