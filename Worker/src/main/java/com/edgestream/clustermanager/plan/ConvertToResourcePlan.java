package com.edgestream.clustermanager.plan;

import com.edgestream.worker.runtime.plan.Resource_Plan;
import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class ConvertToResourcePlan {
    private final String resource_plan_filepath;

    public ConvertToResourcePlan() {
        this.resource_plan_filepath = "./src/JSONProcessor/resource_plan_sample.json";
    }

    public ConvertToResourcePlan(String resource_plan_filepath) {
        this.resource_plan_filepath = resource_plan_filepath;
    }

    public Object JSONtoRP() throws FileNotFoundException {
        // read one JSON file and convert it into an object
        BufferedReader JSONfile_R = new BufferedReader(new FileReader(this.resource_plan_filepath));
        Resource_Plan resource_plan = new Gson().fromJson(JSONfile_R, Resource_Plan.class);

        return resource_plan;
    }
}
