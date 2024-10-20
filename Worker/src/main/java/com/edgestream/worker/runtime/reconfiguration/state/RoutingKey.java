package com.edgestream.worker.runtime.reconfiguration.state;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.common.TupleHeader;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Objects;

public class RoutingKey implements Serializable {


    private Tuple syncTuple;
    private Tuple stableTuple;
    private boolean hasSyncTuple;
    private boolean hasStableTuple;
    private ArrayList<AtomicKey> routingKeyList;
    private String filterString;
    private boolean isAny = false;


    public RoutingKey() {
    }

    public RoutingKey(ArrayList<AtomicKey> keyList){

        for(AtomicKey atomicKey: keyList){
            if (atomicKey.getKey().equalsIgnoreCase("any")){
                this.isAny = true;
                this.routingKeyList = keyList;
                this.filterString = "";
            }
        }

        if(!this.isAny) {
            this.routingKeyList = keyList;
            setFilterString();
        }
    }


    public boolean isAny() {
        return isAny;
    }

    public Tuple getSyncTuple() {
        return syncTuple;
    }

    public Tuple getStableTuple() {
        return stableTuple;
    }

    public boolean hasSyncTuple() {
        return hasSyncTuple;
    }

    public ArrayList<AtomicKey> getAtomicKeyList() {
        return routingKeyList;
    }




    /**
     * This generates a list format this is used for passing to the operator containers as a jar parameter
     */
    public String routingKeyListAsString(){

        StringBuilder keyListAsString = new StringBuilder();

        for (AtomicKey atomicKey: getAtomicKeyList()){

            keyListAsString.append(atomicKey.getKey());
            keyListAsString.append(",");
            keyListAsString.append(atomicKey.getValue());
            keyListAsString.append("|");

        }

        return keyListAsString.toString();
    }


    public String routingKeyAttributeQueueName(){

        StringBuilder keyListAsString = new StringBuilder();

        for (AtomicKey atomicKey: getAtomicKeyList()){

            keyListAsString.append("_");
            keyListAsString.append(atomicKey.getKey());
            keyListAsString.append("_");
            keyListAsString.append(atomicKey.getValue());

        }

        return keyListAsString.toString();
    }


    public String getFilterString(){

        return this.filterString;
    }



//    private void setFilterString(){
//
//        //"(" + routingKey.getKey() + "=" + "'" +routingKey.getValue()+"')";
//
//        StringBuilder filterString = new StringBuilder();
//
//        int arraySize = getAtomicKeyList().size();
//
//        for (int i = 0; i <arraySize; i++) {
//
//            AtomicKey atomicKey = getAtomicKeyList().get(i);
//            filterString.append("(");
//            filterString.append(atomicKey.getKey());
//            filterString.append("=");
//            filterString.append("'");
//            filterString.append(atomicKey.getValue());
//            filterString.append("'");
//            filterString.append(")");
//
//            //if its not the last key in the list then add the AND operator
//            if(i < arraySize-1){
//
//                filterString.append(" OR ");
//
//            }
//
//        }
//
//        this.filterString = filterString.toString();
//    }


    private void setFilterString(){

        //"(" + routingKey.getKey() + "=" + "'" +routingKey.getValue()+"')";

        StringBuilder filterString = new StringBuilder();

        int arraySize = getAtomicKeyList().size();

        for (int i = 0; i <arraySize; i++) {
            AtomicKey atomicKey = getAtomicKeyList().get(i);
            if ( i == 0) {
                filterString.append(atomicKey.getKey());
                filterString.append(" IN (");
            }

            filterString.append("'");
            filterString.append(atomicKey.getValue());
            filterString.append("'");

            //if its not the last key in the list then add the AND operator
            if(i < arraySize-1){

                filterString.append(" , ");

            } else {
                filterString.append(")");
            }

        }

        this.filterString = filterString.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RoutingKey)) return false;
        RoutingKey that = (RoutingKey) o;
        return filterString.equalsIgnoreCase(that.filterString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filterString);
    }

    public boolean hasStableTuple() {return hasStableTuple; }

    //This is ONLY used when creating a new execution plan that requires that this operator be dual routed
    public void activateSynchronization(String topologyID, String inputType, String origin, RoutingKey routingKey){

        hasSyncTuple = true;
        syncTuple = new Tuple();
        syncTuple.setReconfigMarker(true);
        syncTuple.setType(inputType);
        syncTuple.setTopologyID(topologyID);

        stableTuple = new Tuple();
        stableTuple.setStableMarker(true);
        stableTuple.setType(inputType);
        stableTuple.setTopologyID(topologyID);

        String timeStamp = ZonedDateTime.now().toString();

        TupleHeader tupleHeader = new TupleHeader("marker",timeStamp,timeStamp,"noPrevOperator",origin,"noProducer");
        tupleHeader.addRoutingKey(routingKey);
        syncTuple.setTupleHeader(tupleHeader);
        hasSyncTuple = true;

        stableTuple.setTupleHeader(tupleHeader);
        hasStableTuple = true;
    }
}
