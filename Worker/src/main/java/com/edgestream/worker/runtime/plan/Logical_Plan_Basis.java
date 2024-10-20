package com.edgestream.worker.runtime.plan;

import java.util.ArrayList;

public class Logical_Plan_Basis {
    private final String id; // unique ID
    private String name;
    private final String type;
    private final String filepath; //
    private final ArrayList<String> predecessor_ids;
    private final ArrayList<String> successor_ids;

    public Logical_Plan_Basis(String id, String type, String filepath, ArrayList<String> predecessor_ids, ArrayList<String> successor_ids) {
        this.id = id;
        this.type = type;
        this.filepath = filepath;
        this.predecessor_ids = predecessor_ids;
        this.successor_ids = successor_ids;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getFilepath() {
        return filepath;
    }

    public ArrayList<String> getPredecessor_ids() {
        return predecessor_ids;
    }

    public ArrayList<String> getSuccessor_ids() {
        return successor_ids;
    }
}
