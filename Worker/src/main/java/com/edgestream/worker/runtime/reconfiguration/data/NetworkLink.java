package com.edgestream.worker.runtime.reconfiguration.data;

public class NetworkLink {
    private String sourceId;
    private String dstId;
    private double bandwidth;
    private double latency;

    public NetworkLink(String[] args) {
        this.setSourceId(args[0].trim());
        this.setDstId(args[1].trim());
        this.setBandwidth(Double.valueOf(args[2].trim()));
        this.setLatency(Double.valueOf(args[3].trim()));
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getDstId() {
        return dstId;
    }

    public void setDstId(String dstId) {
        this.dstId = dstId;
    }

    public double getBandwidth() {
        return bandwidth;
    }

    public void setBandwidth(double bandwidth) {
        this.bandwidth = bandwidth;
    }

    public double getLatency() {
        return latency;
    }

    public void setLatency(double latency) {
        this.latency = latency;
    }


    public String toTuple() {
        String tuple =
                        sourceId
                        + ";"
                        + dstId
                        + ";"
                        + bandwidth
                        + ";"
                        + latency;

        return tuple;
    }

}
