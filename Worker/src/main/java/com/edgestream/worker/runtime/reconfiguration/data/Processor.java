package com.edgestream.worker.runtime.reconfiguration.data;

public class Processor {
    private String operatorId;
    private String processorName;
    private double refPerc;

    public Processor(String[] args) {
        this.setOperatorId(args[0].trim());
        this.setProcessorName(args[1].trim());
        this.setRefPerc(Double.valueOf(args[2].trim()));
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

    public double getRefPerc() {
        return refPerc;
    }

    public void setRefPerc(double refPerc) {
        this.refPerc = refPerc;
    }

}
