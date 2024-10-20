package com.edgestream.worker.metrics.model;

import java.lang.reflect.Field;

public class TrafficMetric  implements Metric{

    private final String timeStamp;

    private final String localTaskManagerID;
    private final String localTaskManagerIP;

    private final String parentTaskManagerID;
    private final String parentTaskManagerIP;

    private long bytesSent =0l;


    public TrafficMetric(String timeStamp, String localTaskManagerID, String localTaskManagerIP, String parentTaskManagerID, String parentTaskManagerIP, long bytesSent) {
        this.timeStamp = timeStamp;
        this.localTaskManagerID = localTaskManagerID;
        this.localTaskManagerIP = localTaskManagerIP;
        this.parentTaskManagerID = parentTaskManagerID;
        this.parentTaskManagerIP = parentTaskManagerIP;
        this.bytesSent = bytesSent;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public String getLocalTaskManagerID() {
        return localTaskManagerID;
    }

    public String getLocalTaskManagerIP() {
        return localTaskManagerIP;
    }

    public String getParentTaskManagerID() {
        return parentTaskManagerID;
    }

    public String getParentTaskManagerIP() {
        return parentTaskManagerIP;
    }

    public long getBytesSent() {
        return bytesSent;
    }

    public String toString() {

        System.out.println("------------Traffic Metric Details---------------");

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
                +localTaskManagerID
                +";"
                +localTaskManagerIP
                +";"
                +parentTaskManagerID
                +";"
                +parentTaskManagerIP
                +";"
                +bytesSent
                ;

        return tuple;
    }
}
