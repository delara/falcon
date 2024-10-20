package com.edgestream.worker.runtime.network;

import com.edgestream.worker.runtime.network.layer.CoreDC;

import java.util.ArrayList;

public class ChildDataCenter extends DataCenter implements CoreDC {

    DataCenter parentDataCenter;

    public DataCenter getParentDataCenter() {
        return parentDataCenter;
    }

    ArrayList<DataCenter> childDataCenters  = new ArrayList<>();

    public ChildDataCenter(String dataCenterID) {

        super(dataCenterID);
    }

    public void setParentDataCenter(DataCenter parentDataCenter){
        this.parentDataCenter = parentDataCenter;

    }

    public void addChildDataCenter (ChildDataCenter child){
        child.setParentDataCenter(this);
        this.childDataCenters.add(child);

    }

    public void addChildDataCenter (LeafDataCenter child){
        child.setParentDataCenter(this);
        this.childDataCenters.add(child);

    }

    public ArrayList<DataCenter> getChildDataCenters(){
        return childDataCenters;
    }

}
