package com.edgestream.worker.runtime.reconfiguration.data;

public class ComputingResource{
    private String id;
    private int coreNumber;
    private String processorName;
    private double processorFrequency;
    private double availableMemory;

    public ComputingResource(String[] args) {
        this.setId(args[0].trim());
        this.setCoreNumber(Integer.valueOf(args[1].trim()));
        this.setProcessorName(args[2].trim());
        this.setProcessorFrequency(Double.valueOf(args[3].trim()));
        this.setAvailableMemory(Double.valueOf(args[4].trim()));
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getCoreNumber() {
        return coreNumber;
    }

    public void setCoreNumber(int coreNumber) {
        this.coreNumber = coreNumber;
    }

    public String getProcessorName() {
        return processorName;
    }

    public void setProcessorName(String processorName) {
        this.processorName = processorName;
    }

    public double getProcessorFrequency() {
        return processorFrequency;
    }

    public void setProcessorFrequency(double processorFrequency) {
        this.processorFrequency = processorFrequency;
    }

    public double getAvailableMemory() {
        return availableMemory;
    }

    public void setAvailableMemory(double availableMemory) {
        this.availableMemory = availableMemory;
    }


    public String toTuple() {
        String tuple =
                id
                        + ";"
                        + coreNumber
                        + ";"
                        + processorName
                        + ";"
                        + processorFrequency
                        + ";"
                        + availableMemory;

        return tuple;
    }

}
