package com.edgestream.worker.metrics.model;

import com.edgestream.worker.metrics.icmp4j.IcmpPingRequest;
import com.edgestream.worker.metrics.icmp4j.IcmpPingResponse;
import com.edgestream.worker.metrics.icmp4j.IcmpPingUtil;

import java.lang.reflect.Field;

public class NetworkSystemMetric  implements Metric {


    private final String timeStamp;
    private final String taskManagerID;
    private final String parentTaskManagerID;
    private final String parentTaskManagerIP;

    private String linkID;
    private String available_bandwidth_mbps;
    private String average_network_latency;



    public NetworkSystemMetric(String timeStamp, String taskManagerID, String parentTaskManagerID, String parentTaskManagerIP) {
        this.timeStamp = timeStamp;
        this.taskManagerID = taskManagerID;
        this.parentTaskManagerID = parentTaskManagerID;
        this.parentTaskManagerIP = parentTaskManagerIP;

        this.setLinkID();
        this.getAndSetAvgLatency();
        this.getAndSetNetworkInterfaceSpeed();

    }


    public NetworkSystemMetric(String timeStamp, String taskManagerID, String parentTaskManagerID, String parentTaskManagerIP, String linkID, String available_bandwidth_mbps, String average_network_latency) {
        this.timeStamp = timeStamp;
        this.taskManagerID = taskManagerID;
        this.parentTaskManagerID = parentTaskManagerID;
        this.parentTaskManagerIP = parentTaskManagerIP;

        this.linkID = linkID;
        this.available_bandwidth_mbps = available_bandwidth_mbps;
        this.average_network_latency = average_network_latency;

    }



    private void setLinkID(){

        this.linkID = this.taskManagerID + "_" + this.parentTaskManagerID;

    }

    private void getAndSetAvgLatency(){

        this.average_network_latency = pingParent();

    }

    private String pingParent(){

        final IcmpPingRequest request = IcmpPingUtil.createIcmpPingRequest ();
        request.setHost (this.parentTaskManagerIP);
        final IcmpPingResponse response = IcmpPingUtil.executePingRequest (request);

        return String.valueOf(response.getRtt());

    }

    private void getAndSetNetworkInterfaceSpeed(){

        this.available_bandwidth_mbps = "1000"; //TODO: figure out how to do this programatically

    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public String getTaskManagerID() {
        return taskManagerID;
    }

    public String getParentTaskManagerID() {
        return parentTaskManagerID;
    }

    public String getParentTaskManagerIP() {
        return parentTaskManagerIP;
    }

    public String getLinkID() {
        return linkID;
    }

    public String getAvailable_bandwidth_mbps() {
        return available_bandwidth_mbps;
    }

    public String getAverage_network_latency() {
        return average_network_latency;
    }


    public String toString() {

        System.out.println("------------Network Metric Details---------------");

        StringBuilder result = new StringBuilder();
        String newLine = System.getProperty("line.separator");

        result.append( this.getClass().getName() );
        result.append( " Object {" );
        result.append(newLine);

        //determine fields declared in this class only (no fields of superclass)
        Field[] fields = this.getClass().getDeclaredFields();

        //print field names paired with their values
        for ( Field field : fields  ) {
            result.append("  ");
            try {
                result.append( field.getName() );
                result.append(": ");
                //requires access to private field:
                result.append( field.get(this) );
            } catch ( IllegalAccessException ex ) {
                System.out.println(ex);
            }
            result.append(newLine);
        }
        result.append("}");

        return result.toString();
    }

    @Override
    public String toTuple() {

        String tuple =  timeStamp
                        +";"
                        +taskManagerID
                        +";"
                        +parentTaskManagerID
                        +";"
                        +parentTaskManagerIP
                        +";"
                        +linkID
                        +";"
                        +available_bandwidth_mbps
                        +";"
                        +average_network_latency
                        ;

        return tuple;
    }
}
