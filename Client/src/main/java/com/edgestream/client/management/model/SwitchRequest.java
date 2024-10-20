package com.edgestream.client.management.model;


public class SwitchRequest extends ManagementMessage{

    public SwitchRequest(String timeStamp, String sourceIP, String sourceOperatorID, String destinationIP) {
        super(timeStamp, sourceIP, sourceOperatorID, destinationIP);
    }
}
