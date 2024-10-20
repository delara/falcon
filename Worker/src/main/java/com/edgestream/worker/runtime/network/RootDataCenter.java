package com.edgestream.worker.runtime.network;

import com.edgestream.worker.runtime.network.layer.CloudDC;
import java.util.ArrayList;

public class RootDataCenter extends DataCenter implements CloudDC {

    ArrayList<DataCenter> childDataCenters = new ArrayList<>();

    public RootDataCenter(String dataCenterID) {
        super(dataCenterID);
    }

    public void addChildDataCenter (ChildDataCenter child){

        child.setParentDataCenter(this);
        childDataCenters.add(child);

    }

    /**
     * This method is called if you have a 2 layer network, i.e. a Cloud->Edge
     * */
    public void addChildDataCenter (LeafDataCenter child){
        child.setParentDataCenter(this);
        this.childDataCenters.add(child);

    }

    public ArrayList<DataCenter> getChildDataCenters(){
        return childDataCenters;
    }
}
