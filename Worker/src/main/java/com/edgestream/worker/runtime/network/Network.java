package com.edgestream.worker.runtime.network;

import com.edgestream.worker.runtime.plan.Host;
import com.edgestream.worker.config.EdgeStreamGetPropertyValues;

public class Network {

    public Network() {

    }

    public RootDataCenter getAWS2TierNetwork() {

        /** Root Node***/
        String destinationTier_Root = "root";
        String destinationTaskManager_Root = "W001";
        String HOST_Type_Root = "Host";  //place holder value
        String Host_Address_Root = EdgeStreamGetPropertyValues.getCLOUD_ROOT_DATACENTER_IP();   // IP of the Worker/Broker
        String Host_Port_Root = "8161";  //place holder value
        Host W001 = new Host(destinationTier_Root,destinationTaskManager_Root,HOST_Type_Root,Host_Address_Root,Host_Port_Root);

        String destinationTier_leaf1 = "leaf1";
        String destinationTaskManager_leaf1 = "W002";
        String HOST_Type_leaf1 = "Host";  //place holder value
        String Host_Address_leaf1 = EdgeStreamGetPropertyValues.getEDGE1_DATACENTER_IP();   // IP of the Worker/Broker
        String Host_Port_leaf1 = "8161";  //place holder value
        Host W002 = new Host(destinationTier_leaf1,destinationTaskManager_leaf1,HOST_Type_leaf1,Host_Address_leaf1,Host_Port_leaf1);

        String destinationTier_leaf2 = "leaf2";
        String destinationTaskManager_leaf2 = "W003";
        String HOST_Type_leaf2 = "Host";  //place holder value
        String Host_Address_leaf2 = EdgeStreamGetPropertyValues.getEDGE2_DATACENTER_IP();   // IP of the Worker/Broker
        String Host_Port_leaf2 = "8161";  //place holder value
        Host W003 = new Host(destinationTier_leaf2,destinationTaskManager_leaf2,HOST_Type_leaf2,Host_Address_leaf2,Host_Port_leaf2);

        /***** Build Physical Network and assign hosts to them **********/

        //The cloud root
        RootDataCenter rootDataCenter = new RootDataCenter("Root");
        rootDataCenter.addHost(W001);

        ChildDataCenter leaf1 =  new ChildDataCenter("leaf1");
        leaf1.addHost(W002);

        ChildDataCenter leaf2 =  new ChildDataCenter("leaf2");
        leaf2.addHost(W003);

        rootDataCenter.addChildDataCenter(leaf1);
        rootDataCenter.addChildDataCenter(leaf2);

        return rootDataCenter;

    }


    public static void printNetworkView(DataCenter dataCenter, String appender){

        appender = (appender + "->" + dataCenter.getDataCenterID());

        if(dataCenter instanceof RootDataCenter){
            RootDataCenter rootDataCenter = (RootDataCenter)dataCenter;
            for (DataCenter d : rootDataCenter.getChildDataCenters()){
                printNetworkView(d,appender);
            }
        }

        if(dataCenter instanceof ChildDataCenter){
            ChildDataCenter childDataCenter = (ChildDataCenter)dataCenter;
            for (DataCenter d : childDataCenter.getChildDataCenters()){
                printNetworkView(d,appender);
            }
        }
        if(dataCenter instanceof LeafDataCenter){
            System.out.println(appender);
        }



    }



}
