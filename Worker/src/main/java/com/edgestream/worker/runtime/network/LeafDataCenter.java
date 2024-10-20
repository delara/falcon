package com.edgestream.worker.runtime.network;

import com.edgestream.worker.runtime.network.layer.EdgeDC;

public class LeafDataCenter extends DataCenter implements EdgeDC {


    DataCenter parentDataCenter;

    public LeafDataCenter(String dataCenterID) {
        super(dataCenterID);

    }

    public void setParentDataCenter(DataCenter parentDataCenter){
        this.parentDataCenter = parentDataCenter;

    }


}
