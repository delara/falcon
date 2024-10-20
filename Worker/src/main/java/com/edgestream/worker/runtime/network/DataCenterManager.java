package com.edgestream.worker.runtime.network;


import com.edgestream.worker.metrics.db.cassandra.CloudMetricsCassandraDB;
import com.edgestream.worker.metrics.db.pathstore.CloudMetricsPathstoreDB;
import com.edgestream.worker.metrics.cloudDB.CloudMetricsDB;


public class DataCenterManager {

    private final RootDataCenter  rootDataCenter;
    private DataCenter dataCenterFound;
    private CloudMetricsDB cloudMetricsDB;
    private final CloudMetricsCassandraDB cloudMetricsCassandraDB = null;
    private final CloudMetricsPathstoreDB cloudMetricsPathstoreDB = null;

    public DataCenterManager(RootDataCenter  rootDataCenter) {

        //Set the root data center
        this.rootDataCenter = rootDataCenter;

        //Create the derby database
        try {
            cloudMetricsDB = new CloudMetricsDB();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        /* TODO: this has been commented out, as of Feb 7 2022 this causes runtime errors, needs to be debugged
        try {
            cloudMetricsPathstoreDB = new CloudMetricsPathstoreDB();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        */
    }

    public CloudMetricsPathstoreDB getMetricsPathstoreDB() {
        return cloudMetricsPathstoreDB;
    }

    public CloudMetricsCassandraDB getMetricsCassandraDB() {
        return cloudMetricsCassandraDB;
    }

    public CloudMetricsDB getMetricsDB() {
        return cloudMetricsDB;
    }

    public RootDataCenter getRootDataCenter() {
        return rootDataCenter;
    }

    public DataCenter findMatchingDataCenter(String dataCenterID){

        //1. search the tree, if not found create a new data center
        DataCenter foundDataCenter = null;
        foundDataCenter =  getMatchingDataCenter(this.rootDataCenter, dataCenterID);

        return  foundDataCenter;

    }


    //TODO: This may be buggy. Need to test with multiple core layers
    private DataCenter getMatchingDataCenter(DataCenter dataCenter, String dataCenterID){


        if(dataCenter instanceof RootDataCenter){
            RootDataCenter rootDataCenter = (RootDataCenter)dataCenter;
            if (rootDataCenter.getDataCenterID().equalsIgnoreCase(dataCenterID)){
                this.dataCenterFound = rootDataCenter;
            }else{
                for (DataCenter d : rootDataCenter.getChildDataCenters()) {
                     if (dataCenterID.equalsIgnoreCase(d.getDataCenterID())) {
                         this.dataCenterFound = d;
                     }else {
                         getMatchingDataCenter(d, dataCenterID);
                     }

                }
            }


        }

        if(dataCenter instanceof ChildDataCenter){
            ChildDataCenter childDataCenter = (ChildDataCenter)dataCenter;
            if (childDataCenter.getDataCenterID().equalsIgnoreCase(dataCenterID)){
                this.dataCenterFound = childDataCenter;

            }else {
                for (DataCenter d : childDataCenter.getChildDataCenters()) {
                    if (d.getDataCenterID().equalsIgnoreCase(dataCenterID)) {
                        this.dataCenterFound = d;
                    } else {
                        getMatchingDataCenter(d, dataCenterID);
                    }

                }
            }
        }

        if(dataCenter instanceof LeafDataCenter){
            if(dataCenterID.equalsIgnoreCase(dataCenter.getDataCenterID())) {
                this.dataCenterFound = dataCenter;
            }
        }


        return this.dataCenterFound;
    }

}
