package com.edgestream.worker.runtime.topology;

import com.edgestream.worker.operator.Operator;
import com.edgestream.worker.runtime.reconfiguration.state.AtomicKey;
import com.edgestream.worker.runtime.reconfiguration.state.RoutingKey;
import org.apache.activemq.artemis.api.core.management.AddressControl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TopologyOperatorAddressManager {

    private final String topologyOperatorAddress;
    private String tupleType;
    private final ActiveMQOperatorAddressFilterExpression anyOperatorFilterExpression = new ActiveMQOperatorAddressFilterExpression(); //aka the any operator filter, it consumes all tuples that wont be consumed by other addressed
    private final HashMap<RoutingKey,TopologyAttributeAddressManager> routingKeyToAddressMapping = new HashMap<>();
    private final ArrayList<String> installedAttributeDiverts = new ArrayList<>();
    private final String anyTupleTypesDivertName;
    boolean forwardToNoMatchActive;
    private final Topology boundTopology;
    private boolean isNewOperatorAddress = true;
    private AddressControl addressController;


    public TopologyOperatorAddressManager(String topologyOperatorAddress, Topology boundTopology) {
        this.topologyOperatorAddress = topologyOperatorAddress;
        this.anyTupleTypesDivertName = "any_tuple_divert_for_" + topologyOperatorAddress;
        this.forwardToNoMatchActive = true;
        this.boundTopology = boundTopology;

    }

    public boolean isNewOperatorAddress() {
        return isNewOperatorAddress;
    }

    private void operatorAddressSetupComplete(){

        this.isNewOperatorAddress = false;
    }



    public void setAddressController(AddressControl addressController){

        this.addressController = addressController;

    }


    public void pauseOperatorAddress(){
        try {
            System.out.println("Pausing address: [" + topologyOperatorAddress +"]" );
            addressController.pause();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void resumeOperatorAddress(){
        try {
            System.out.println("Resuming operator address: " + topologyOperatorAddress);
            addressController.resume();

            if(isNewOperatorAddress()) {
                operatorAddressSetupComplete();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Topology getBoundTopology(){
        return boundTopology;
    }

    public void setTupleType(String tupleType) {
        this.tupleType = tupleType;
    }

    public String getTupleType() {
        return tupleType;
    }

    public String getAddress() {
        return topologyOperatorAddress;
    }

    public String getAnyTupleTypesDivertName() {
        return anyTupleTypesDivertName;
    }

    public ArrayList<String> getInstalledAttributeDiverts() {
        return installedAttributeDiverts;
    }


    public void installAttributeDivert(String divertName){
        this.installedAttributeDiverts.add(divertName);

    }

    public boolean isForwardToNoMatchActive(){
        return this.forwardToNoMatchActive;
    }

    public void deactivateForwardToNoMatch(){
        this.forwardToNoMatchActive = false;
    }
    public void activateForwardToNoMatch(){
        this.forwardToNoMatchActive = true;
    }

    public HashMap<RoutingKey, TopologyAttributeAddressManager> getRoutingKeyToAddressMapping() {
        return routingKeyToAddressMapping;
    }


    public String getAnyOperatorFilterExpression() {
        return anyOperatorFilterExpression.getNoMatchFilterExpression();
    }



    /**************************************************************************
     *
     *                       Attribute Management
     *
     **************************************************************************/

    public TopologyAttributeAddressManager getTopologyAttributeAddressManager(RoutingKey routingKey){

        RoutingKey routingKeyInstance = getRoutingKeyInstance(routingKey);
        TopologyAttributeAddressManager topologyAttributeAddressManager = routingKeyToAddressMapping.get(routingKeyInstance);
        return topologyAttributeAddressManager;

    }


    private RoutingKey getRoutingKeyInstance(RoutingKey routingKeyReference){

        RoutingKey routingKeyInstance = null;

        if(routingKeyExists(routingKeyReference)){

            Map mp = this.routingKeyToAddressMapping;

            Iterator it = mp.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry)it.next();
                //System.out.println(pair.getKey() + " = " + pair.getValue());
                RoutingKey routingKeyFromMap = (RoutingKey) pair.getKey();
                if(routingKeyFromMap.equals(routingKeyReference)){
                    routingKeyInstance = routingKeyFromMap;
                }
                //it.remove(); // avoids a ConcurrentModificationException
            }
        }
        return routingKeyInstance;
    }

    public boolean routingKeyExists(RoutingKey routingKeyToFind) {

        boolean keyExists = false;

        Map mp = this.routingKeyToAddressMapping;

        Iterator it = mp.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            //System.out.println(pair.getKey() + " = " + pair.getValue());
            RoutingKey routingKeyFromMap = (RoutingKey) pair.getKey();
            if(routingKeyFromMap.equals(routingKeyToFind)){
                keyExists = true;
            }

            //it.remove(); // avoids a ConcurrentModificationException
        }

        return keyExists;
    }



    private static void printMap(Map mp) {
        Iterator it = mp.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            System.out.println(pair.getKey() + " = " + pair.getValue());
            it.remove(); // avoids a ConcurrentModificationException
        }
    }



    public void removeRoutingKey(RoutingKey routingKey){
        this.anyOperatorFilterExpression.removeRoutingKey(routingKey);
        this.routingKeyToAddressMapping.remove(routingKey);
    }


    /**
     * This function is only called by the TaskBrokerConfig.setupAttributeAddress() method.
     * This is expected to be called after the actual address has been setup on the broker
     * @param routingKey
     * @param operator
     * @param attributeAddress
     */
    public void addAttributeAddress(RoutingKey routingKey, Operator operator, String attributeAddress){

        System.out.println(System.currentTimeMillis() + " Attempting to add new attribute address " +attributeAddress +" to TopologyAttributeAddressManager....");
        this.routingKeyToAddressMapping.put(routingKey,new TopologyAttributeAddressManager(routingKey,attributeAddress,this));

        System.out.println(System.currentTimeMillis() + " Attribute Address has been added");

        if(!routingKey.isAny()) {
            this.anyOperatorFilterExpression.addRoutingKey(routingKey);
            installAttributeDivert(getTopologyAttributeAddressManager(routingKey).getDivertName());
        }
    }


    public boolean hasActiveAnyOperator(){

        boolean hasActiveAnyOperator = false;

        ArrayList<AtomicKey> keyList = new ArrayList<>();
        keyList.add(new AtomicKey("any","any"));
        RoutingKey anyRoutingKey = new RoutingKey(keyList);


        if(routingKeyExists(anyRoutingKey)){

            TopologyAttributeAddressManager topologyAttributeAddressManager = getTopologyAttributeAddressManager(anyRoutingKey);
            if(!topologyAttributeAddressManager.getOperatorsOnThisAddress().isEmpty()){
                hasActiveAnyOperator = true;
            }
        }
        return  hasActiveAnyOperator;
    }





    public void resumeAllAttributeAddresses(){

        Iterator it = routingKeyToAddressMapping.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            TopologyAttributeAddressManager topologyAttributeAddressManager = (TopologyAttributeAddressManager)pair.getValue();
            topologyAttributeAddressManager.resumeAttributeAddress();

        }
    }

}
