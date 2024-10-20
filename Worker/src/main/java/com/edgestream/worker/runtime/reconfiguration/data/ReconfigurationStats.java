package com.edgestream.worker.runtime.reconfiguration.data;


import com.edgestream.worker.metrics.model.OperatorStateMetric;

import java.util.ArrayList;

public class ReconfigurationStats {
    private final ArrayList<ComputingResource> computingResources;
    private final ArrayList<Edge> edges;
    private final ArrayList<NetworkLink> networkLinks;
    private final ArrayList<OperatorMap> operatorMapping;
    private final ArrayList<OperatorStatistics> operatorStatistics;


    public ReconfigurationStats(ArrayList<ComputingResource> computingResources, ArrayList<Edge> edges, ArrayList<NetworkLink> networkLinks
            , ArrayList<OperatorMap> operatorMapping, ArrayList<OperatorStatistics> operatorStatistics) {

        this.computingResources = computingResources;
        this.edges = edges;
        this.networkLinks = networkLinks;
        this.operatorMapping = operatorMapping;
        this.operatorStatistics = operatorStatistics;
    }


    public ArrayList<ComputingResource> getComputingResources() {
        return computingResources;
    }

    public ArrayList<Edge> getEdges() {
        return edges;
    }

    public ArrayList<NetworkLink> getNetworkLinks() {
        return networkLinks;
    }

    public ArrayList<OperatorMap> getOperatorMapping() {
        return operatorMapping;
    }

    public ArrayList<OperatorStatistics> getOperatorStatistics() {
        return operatorStatistics;
    }
}
