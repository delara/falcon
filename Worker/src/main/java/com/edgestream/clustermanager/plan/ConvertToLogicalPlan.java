package com.edgestream.clustermanager.plan;


import com.edgestream.worker.runtime.plan.Logical_Plan;
import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class ConvertToLogicalPlan {
    private final String logical_plan_filepath;

    public ConvertToLogicalPlan() {
        this.logical_plan_filepath = "./src/JSONProcessor/logical_plan_sample.json";
    }

    public ConvertToLogicalPlan(String logical_Plan_filepath) {
        this.logical_plan_filepath = logical_Plan_filepath;
    }

    public Object JSONtoLP() throws FileNotFoundException {
        // read one JSON file and convert it into an object
        BufferedReader JSONfile_L = new BufferedReader(new FileReader(this.logical_plan_filepath));
        Logical_Plan logical_plan = new Gson().fromJson(JSONfile_L, Logical_Plan.class);

        return logical_plan;
        }
}
