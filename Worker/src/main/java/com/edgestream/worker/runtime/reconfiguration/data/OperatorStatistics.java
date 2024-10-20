package com.edgestream.worker.runtime.reconfiguration.data;



import java.util.ArrayList;
import java.util.StringTokenizer;

public class OperatorStatistics {
    private String id;
    private String replicaId;
    private String resourceID;
    //Metrics per second
    private double cpuUsage;
    private double memUsage;
    private int inputNumberTuples;
    private int outputNumberTuples;
    private double inputSizeTuple;
    private double outputSizeTuple;
    private double processingTime;
    private int actionNumber = 1;
    private String tupleOriginList;

    public OperatorStatistics(String[] args) {
        this.setId(args[0].trim());
        this.setReplicaId(args[1].trim());
        this.setResourceID(args[2].trim());
        try {
            this.setCpuUsage(Double.valueOf(args[3].trim()));
            this.setMemUsage(Double.valueOf(args[4].trim()));
        }catch(NullPointerException exception){
            System.out.println("Cpu Results are missing, setting to zero (0.0)");
            this.setCpuUsage(0.00);
            this.setMemUsage(0.00);

        }
        //this.setInputNumberTuples(Integer.valueOf(args[5].trim()));
        //this.setOutputNumberTuples(Integer.valueOf(args[6].trim()));
        this.setInputNumberTuples((int)(Float.parseFloat(args[5].trim())));
        this.setOutputNumberTuples((int)(Float.parseFloat(args[6].trim())));
        this.setInputSizeTuple(Double.valueOf(args[7].trim()));
        this.setOutputSizeTuple(Double.valueOf(args[8].trim()));
        this.setProcessingTime(Double.valueOf(args[9].trim()));
        this.setTupleOriginList(args[10]);




    }

    public void setTupleOriginList(String tupleOriginList) {
        this.tupleOriginList = tupleOriginList;
    }

    public ArrayList<String> getOriginList() {
        StringTokenizer st = new StringTokenizer(this.tupleOriginList, "|");
        ArrayList<String> tupleOriginLocations = new ArrayList<>();


        while (st.hasMoreTokens()) {
            tupleOriginLocations.add(st.nextToken());

        }
        return tupleOriginLocations;

    }


    public String getReplicaId() {
        return replicaId;
    }

    public void setReplicaId(String replicaId) {
        this.replicaId = replicaId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getResourceID() {
        return resourceID;
    }

    public void setResourceID(String resourceID) {
        this.resourceID = resourceID;
    }

    public double getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(double cpuUsage) {
        this.cpuUsage = cpuUsage;
    }

    public double getMemUsage() {
        return memUsage;
    }

    public void setMemUsage(double memUsage) {
        this.memUsage = memUsage;
    }

    public int getInputNumberTuples() {
        return inputNumberTuples;
    }

    public void setInputNumberTuples(int inputNumberTuples) {
        this.inputNumberTuples = inputNumberTuples;
    }

    public int getOutputNumberTuples() {
        return outputNumberTuples;
    }

    public void setOutputNumberTuples(int outputNumberTuples) {
        this.outputNumberTuples = outputNumberTuples;
    }

    public double getInputSizeTuple() {
        return inputSizeTuple;
    }

    public void setInputSizeTuple(double inputSizeTuple) {
        this.inputSizeTuple = inputSizeTuple;
    }

    public double getOutputSizeTuple() {
        return outputSizeTuple;
    }

    public void setOutputSizeTuple(double outputSizeTuple) {
        this.outputSizeTuple = outputSizeTuple;
    }

    public double getProcessingTime() {
        return processingTime;
    }

    public void setProcessingTime(double processingTime) {
        this.processingTime = processingTime;
    }

    public int getActionNumber() {
        return actionNumber;
    }

    public void setActionNumber(int actionNumber) {
        this.actionNumber = actionNumber;
    }

    public ArrayList<String> getTupleOriginStringAsList(){

        ArrayList<String> originList = new ArrayList<>();

        StringTokenizer st = new StringTokenizer(this.tupleOriginList,"|");
        while (st.hasMoreTokens()) {
            originList.add(st.nextToken());
        }

        return originList;
    }


    public String toTuple() {
        String tuple =
                        id
                        + ";"
                        + resourceID
                        + "; CPU % ["
                        + cpuUsage
                        + "]; MEM Bytes["
                        + memUsage
                        + "]; inputNumberTuples ["
                        + inputNumberTuples
                        + "]; outputNumberTuples ["
                        + outputNumberTuples
                        + "]; inputSizeTuple ["
                        + inputSizeTuple
                        + "]; outputSizeTuple ["
                        + outputSizeTuple
                        + "]; processingTime ["
                        + processingTime
                        + "]; tupleOriginList ["
                        + tupleOriginList
                        + "]"
                        ;


        return tuple;

    }





}
