package com.edgestream.worker.runtime.network;


import com.edgestream.worker.config.EdgeStreamGetPropertyValues;
import com.edgestream.worker.runtime.plan.Host;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DataCenter {

    HashMap<String, Host> managedHosts = new HashMap<>();
    final String dataCenterID;


    public DataCenter(String dataCenterID) {

        this.dataCenterID = dataCenterID;

    }

    public String getDataCenterID() {
        return dataCenterID;
    }

    public void addHost(Host host){

       managedHosts.put(host.getTask_Manager_ID(), host);


    }

    public HashMap<String, Host> getManagedHostsMap() {
        return managedHosts;
    }

    public ArrayList<Host> getManageHostsList(){

        ArrayList<Host> manageHostsList = new ArrayList<>();


        Iterator hmIterator = managedHosts.entrySet().iterator();


        while (hmIterator.hasNext()) {
            Map.Entry mapElement = (Map.Entry)hmIterator.next();
            Host host = ((Host)mapElement.getValue());
            //System.out.println(host.getTask_Manager_ID());
            manageHostsList.add(host);
        }

        return manageHostsList;
    }


    public DataCenter searchForDataCenterByTaskManagerID(RootDataCenter rootDataCenter, String taskManagerID){

        HashMap<String,DataCenter> dataCenterToHostMapping = new HashMap<>();
        DataCenter foundDataCenter = null;


        if(EdgeStreamGetPropertyValues.getNETWORK_LAYOUT().equals("cslab_3tier")) {
            /*** 3 tier 7 node Syslab layout**/
             dataCenterToHostMapping.put("W001",rootDataCenter);
             dataCenterToHostMapping.put("W002",rootDataCenter.getChildDataCenters().get(0));
             dataCenterToHostMapping.put("W003",rootDataCenter.getChildDataCenters().get(1));

             ChildDataCenter core1 = (ChildDataCenter)rootDataCenter.getChildDataCenters().get(0);
             ChildDataCenter core2 = (ChildDataCenter)rootDataCenter.getChildDataCenters().get(1);

             dataCenterToHostMapping.put("W004",core1.getChildDataCenters().get(0));
             dataCenterToHostMapping.put("W005",core1.getChildDataCenters().get(1));
             dataCenterToHostMapping.put("W006",core2.getChildDataCenters().get(0));
             dataCenterToHostMapping.put("W007",core2.getChildDataCenters().get(1));

             foundDataCenter = dataCenterToHostMapping.get(taskManagerID);


        }

        if(EdgeStreamGetPropertyValues.getNETWORK_LAYOUT().equals("aws")) {
            /***3 iter 3 node AWS deployment***/

            dataCenterToHostMapping.put("W001", rootDataCenter);
            dataCenterToHostMapping.put("W002", rootDataCenter.getChildDataCenters().get(0));
            //dataCenterToHostMapping.put("W003",rootDataCenter.getChildDataCenters().get(1));

            ChildDataCenter core1 = (ChildDataCenter) rootDataCenter.getChildDataCenters().get(0);
            //ChildDataCenter core2 = (ChildDataCenter)rootDataCenter.getChildDataCenters().get(1);

            dataCenterToHostMapping.put("W003", core1.getChildDataCenters().get(0));
            //dataCenterToHostMapping.put("W004",core1.getChildDataCenters().get(0));
            //dataCenterToHostMapping.put("W005",core1.getChildDataCenters().get(1));
            //dataCenterToHostMapping.put("W006",core2.getChildDataCenters().get(0));
            //dataCenterToHostMapping.put("W007",core2.getChildDataCenters().get(1));

            foundDataCenter = dataCenterToHostMapping.get(taskManagerID);


        }

        if(EdgeStreamGetPropertyValues.getNETWORK_LAYOUT().equals("aws2t")) {

            dataCenterToHostMapping.put("W001", rootDataCenter);
            dataCenterToHostMapping.put("W002", rootDataCenter.getChildDataCenters().get(0));
            dataCenterToHostMapping.put("W003", rootDataCenter.getChildDataCenters().get(1));

            foundDataCenter = dataCenterToHostMapping.get(taskManagerID);
        }


        if(EdgeStreamGetPropertyValues.getNETWORK_LAYOUT().equals("aws_geo")) {
            /***AWS GEO deployment***/

            dataCenterToHostMapping.put("W001", rootDataCenter);
            dataCenterToHostMapping.put("W002", rootDataCenter.getChildDataCenters().get(0));
            dataCenterToHostMapping.put("W004", rootDataCenter.getChildDataCenters().get(1));

            ChildDataCenter core1 = (ChildDataCenter) rootDataCenter.getChildDataCenters().get(0);


            dataCenterToHostMapping.put("W005", core1.getChildDataCenters().get(0));

            foundDataCenter = dataCenterToHostMapping.get(taskManagerID);


        }



        if(EdgeStreamGetPropertyValues.getNETWORK_LAYOUT().equals("2t")) {
            /***AWS 2t deployment***/

            dataCenterToHostMapping.put("W001", rootDataCenter);
            dataCenterToHostMapping.put("W002", rootDataCenter.getChildDataCenters().get(0));
            dataCenterToHostMapping.put("W003", rootDataCenter.getChildDataCenters().get(1));
            dataCenterToHostMapping.put("W004", rootDataCenter.getChildDataCenters().get(2));
            dataCenterToHostMapping.put("W005", rootDataCenter.getChildDataCenters().get(3));
            dataCenterToHostMapping.put("W006", rootDataCenter.getChildDataCenters().get(4));

            foundDataCenter = dataCenterToHostMapping.get(taskManagerID);

        }

        return foundDataCenter;

        //TODO: fix this algorithm after paper
        /*

        if(targetDataCenter==null) {

            if (dataCenter instanceof RootDataCenter && targetDataCenter==null) {
                RootDataCenter rootDataCenter = (RootDataCenter) dataCenter;
                for (DataCenter d : rootDataCenter.getChildDataCenters()) {
                    if (d.getManagedHostsMap().containsKey(taskManagerID) && targetDataCenter==null) {
                        targetDataCenter = d;
                    } else {
                        searchForDataCenterByTaskManagerID(d, taskManagerID, targetDataCenter);
                    }
                }

            }

            if (dataCenter instanceof ChildDataCenter && targetDataCenter==null) {
                ChildDataCenter childDataCenter = (ChildDataCenter) dataCenter;
                for (DataCenter d : childDataCenter.getChildDataCenters()) {
                    if (d.getManagedHostsMap().containsKey(taskManagerID) && targetDataCenter==null) {
                        targetDataCenter = d;
                    } else {
                        searchForDataCenterByTaskManagerID(d, taskManagerID, targetDataCenter);
                    }
                }
            }
            if (dataCenter instanceof LeafDataCenter && targetDataCenter==null) {
                if (dataCenter.getManagedHostsMap().containsKey(taskManagerID)) {
                    targetDataCenter = dataCenter;

                }
            }

        }

        return targetDataCenter;


         */


    }


    /**
     * This method gives you the list of ALL task managers in the Network
      * @param dataCenter
     * @param taskManagerList
     * @return
     */
    public ArrayList<String> getTaskManagerList(DataCenter dataCenter, ArrayList<String> taskManagerList){

        ArrayList<String> allTaskManagers = null;

        HashMap<String, Host> hosts = dataCenter.getManagedHostsMap();

        Iterator hmIterator = hosts.entrySet().iterator();


        while (hmIterator.hasNext()) {
            Map.Entry mapElement = (Map.Entry)hmIterator.next();
            Host host = ((Host)mapElement.getValue());
            //System.out.println(host.getTask_Manager_ID());
            taskManagerList.add(host.getTask_Manager_ID());
        }


        //taskManagerList.add(dataCenter.getDataCenterID());


        if(dataCenter instanceof RootDataCenter){
            RootDataCenter rootDataCenter = (RootDataCenter)dataCenter;
            for (DataCenter d : rootDataCenter.getChildDataCenters()){
                getTaskManagerList(d,taskManagerList);
            }
            allTaskManagers = taskManagerList; //set the return value once you have finished traversing the tree
        }

        if(dataCenter instanceof ChildDataCenter){
            ChildDataCenter childDataCenter = (ChildDataCenter)dataCenter;
            for (DataCenter d : childDataCenter.getChildDataCenters()){
                getTaskManagerList(d,taskManagerList);
            }
        }
        if(dataCenter instanceof LeafDataCenter){
            //System.out.println(taskManagerList);
        }

        return allTaskManagers;

    }


}
