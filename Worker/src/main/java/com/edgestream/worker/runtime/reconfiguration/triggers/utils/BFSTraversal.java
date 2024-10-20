package com.edgestream.worker.runtime.reconfiguration.triggers.utils;


import com.edgestream.worker.runtime.reconfiguration.data.Edge;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

public class BFSTraversal {
    private ArrayList<String> sources = new ArrayList<>();
    private ArrayList<String> orderedList = new ArrayList<>();

    public BFSTraversal(ArrayList<Edge> edges) {
        this.getDeploymentList(edges);
    }

    public void getDeploymentList(ArrayList<Edge> edges) {
        ArrayList<String> list = new ArrayList<>();
        Queue<String> q = new LinkedList<>();

        //Get Sources
        for (Edge e : edges) {
            int i = 0;
            for (Edge c : edges) {
//                System.out.println("Ope: " + e.getOperator_id() + " Child:" + c.getChild_id());
                if (e.getOperator_id().equals(c.getChild_id())) {
                    i = 1;
                    break;
                }
            }

            if (i == 0) {
                if (!isExistingValue(list, e.getOperator_id())) {
                    list.add(e.getOperator_id());
                    this.getSources().add(e.getOperator_id());

                    for (Edge c : edges) {
                        if (e.getOperator_id().equals(c.getOperator_id())) {
                            q.add(c.getChild_id());
                        }
                    }

                }
            }
        }

        //
        while (!q.isEmpty()) {
            String ope = q.remove();

            if (!isContrained(ope, list, edges)) {
                if (!isExistingValue(list, ope)) {
                    list.add(ope);
                    for (Edge c : edges) {
                        if (ope.equals(c.getOperator_id())) {
                            q.add(c.getChild_id());
                        }
                    }
                }
            } else {
                q.add(ope);
            }

        }

        this.setOrderedList(list);
    }

    private boolean isExistingValue(ArrayList<String> list, String value) {
        for (String v : list) {
            if (v.equals(value)) {
                return true;
            }
        }
        return false;
    }

    private boolean isContrained(String operator, ArrayList<String> list, ArrayList<Edge> edges) {
        for (Edge e : edges) {
            if (e.getChild_id() == operator) {
                boolean valid = false;
                for (String l : list) {
                    if (l.equals(e.getOperator_id())) {
                        valid = true;
                    }
                }
                if (!valid){
                    return true;
                }
            }
        }

        return false;
    }

    public ArrayList<String> getOrderedList() {
        return orderedList;
    }

    public void setOrderedList(ArrayList<String> orderedList) {
        this.orderedList = orderedList;
    }

    public ArrayList<String> getSources() {
        return sources;
    }

    public void setSources(ArrayList<String> sources) {
        this.sources = sources;
    }
}
