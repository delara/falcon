package com.edgestream.worker.runtime.reconfiguration.data;

public class ProcessorOperatorStat {
    private String operatorId;
    private String processorName;
    private double cpuUsage;
    private long tuples;

    public ProcessorOperatorStat(String[] args) {
        this.setOperatorId(args[0].trim());
        this.setProcessorName(args[1].trim());
        this.setCpuUsage(Double.valueOf(args[2].trim()));
        this.setTuples(Long.valueOf(args[3].trim()));

    }

    public String getOperatorId() {
        return operatorId;
    }

    public void setOperatorId(String operatorId) {
        this.operatorId = operatorId;
    }

    public String getProcessorName() {
        return processorName;
    }

    public void setProcessorName(String processorName) {
        this.processorName = processorName;
    }

    public double getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(double cpuUsage) {
        this.cpuUsage = cpuUsage;
    }

    public long getTuples() {
        return tuples;
    }

    public void setTuples(long tuples) {
        this.tuples = tuples;
    }

}
