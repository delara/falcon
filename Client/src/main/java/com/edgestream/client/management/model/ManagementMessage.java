package com.edgestream.client.management.model;

public class ManagementMessage {

    private final String timeStamp;
    private final String sourceIP;
    private final String sourceOperatorID;
    private final String destinationIP;

    public ManagementMessage(String timeStamp, String sourceIP, String sourceOperatorID, String destinationIP) {
        this.timeStamp = timeStamp;
        this.sourceIP = sourceIP;
        this.sourceOperatorID = sourceOperatorID;
        this.destinationIP = destinationIP;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public String getSourceIP() {
        return sourceIP;
    }

    public String getSourceOperatorID() {
        return sourceOperatorID;
    }

    public String getDestinationIP() {
        return destinationIP;
    }
}
