package com.edgestream.worker.runtime.topology;

import com.edgestream.worker.runtime.reconfiguration.state.RoutingKey;

import java.util.ArrayList;

/**
 *
 * This ActiveMQOperatorAddressFilterExpression is used by the calling TopologyOperatorAddressManager to determine
 * which tuples matching the routing key attributes attributes should NOT be forwarded to specific attribute addresses installed on this broker.
 * When a new routing key is added, the filter expression will be updated to now check the header of all messages arriving
 * and send any messages to the ANY attribute address.
 *
 */
public class ActiveMQOperatorAddressFilterExpression {

    private String noMatchFilterExpression;
    private final ArrayList<RoutingKey> routingKeys = new ArrayList<>();



    public ActiveMQOperatorAddressFilterExpression() {

    }


    String getNoMatchFilterExpression(){

        return  noMatchFilterExpression;
    }

    /**
    private void updateFilterExpression() {

        String filter_string = null;
        for(RoutingKey routingKey : routingKeys){

            String expressionFragment = "("+ routingKey.getKey() +"='" + routingKey.getValue() + "')";
            //1. The first filter does not have AND preceding it, so just all the filter string
            if(filter_string == null) {
                filter_string = "NOT ";
                filter_string = filter_string + expressionFragment;

            }else{
                filter_string = filter_string + " AND NOT " + expressionFragment;

            }
        }

        this.noMatchFilterExpression = filter_string;
        System.out.println("New divert created on operator address: " + noMatchFilterExpression );

    }


    public boolean addRoutingKey(RoutingKey routingKeyToAdd){


        boolean foundKey = false;

        for(RoutingKey routingKey: this.routingKeys){
            if(routingKey.getKey().equalsIgnoreCase(routingKeyToAdd.getKey())
            && routingKey.getValue().equalsIgnoreCase(routingKeyToAdd.getValue())){
                System.out.println("Divert with key : "+ routingKey.getKey()+"  and value: " +routingKey.getValue() + " already exists, will not create.");
                foundKey = true;
                break;
            }
        }

        if(!foundKey) {

            routingKeys.add(routingKeyToAdd);
            System.out.println("Divert with key : "+ routingKeyToAdd.getKey()+"  and value: " +routingKeyToAdd.getValue() + " already does not yet exist, will create!");
            updateFilterExpression();
        }

        return foundKey;
    }



    public void removeRoutingKey(RoutingKey routingKeyToRemove){

        boolean foundKey = false;

        for(RoutingKey routingKey: this.routingKeys){
            if(routingKey.getKey().equalsIgnoreCase(routingKeyToRemove.getKey())
                    && routingKey.getValue().equalsIgnoreCase(routingKeyToRemove.getValue())){
                System.out.println("Divert with key : "+ routingKey.getKey()+"  and value: " +routingKey.getValue() + " has been found, it will be removed.");
                foundKey = true;
                updateFilterExpression();
                break;
            }
        }

        if(!foundKey) {
            System.out.println("Divert with key : " + routingKeyToRemove.getKey() + "  and value: " + routingKeyToRemove.getValue() + "does not exist, check your logic for errors.");
        }
    }
    */




    /**
     *
     * This filter expression is placed on the ANY address for this operator type. The ANY address processes all tuples
     * that are not intended to go to a specific attribute address.
     *
     */
    private void updateFilterExpression() {

        String filter_string = null;
        for(RoutingKey routingKey : routingKeys){

            String expressionFragment = routingKey.getFilterString();
            //1. The first filter does not have AND preceding it, so just all the filter string
            if(filter_string == null) {
                filter_string = " NOT (";
                filter_string = filter_string + expressionFragment;

//                filter_string = "vehicNOT IN (";
//                filter_string = filter_string + expressionFragment;

            }else{
//                filter_string = filter_string + " AND NOT " + expressionFragment;

            }


        }
        filter_string = filter_string + ")";
        this.noMatchFilterExpression = filter_string;
        System.out.println(System.currentTimeMillis() + " New divert created on operator address: " + noMatchFilterExpression );

    }


    public boolean addRoutingKey(RoutingKey routingKeyToAdd){


        boolean foundKey = false;

        for(RoutingKey routingKey: this.routingKeys){
            if(routingKey.equals(routingKeyToAdd)){
                System.out.println(System.currentTimeMillis() + "Divert with filter pattern : "+ routingKey.getFilterString() + " already exists, will not create.");
                foundKey = true;
                break;
            }
        }

        if(!foundKey) {

            routingKeys.add(routingKeyToAdd);
            System.out.println(System.currentTimeMillis() + "Divert with filter pattern : "+ routingKeyToAdd.getFilterString() + " does not yet exist, will create!");
            updateFilterExpression();
        }

        return foundKey;
    }



    public void removeRoutingKey(RoutingKey routingKeyToRemove){

        boolean foundKey = false;

        for(RoutingKey routingKey: this.routingKeys){
            if(routingKey.equals(routingKeyToRemove)){
                System.out.println(System.currentTimeMillis() + " Divert with filter pattern : "+ routingKey.getFilterString() + " has been found, it will be removed.");
                foundKey = true;
                updateFilterExpression();
                break;
            }
        }

        if(!foundKey) {
            System.out.println("Divert with filter pattern : " + routingKeyToRemove.getFilterString() + "does not exist, check your logic for errors.");
        }
    }

}
