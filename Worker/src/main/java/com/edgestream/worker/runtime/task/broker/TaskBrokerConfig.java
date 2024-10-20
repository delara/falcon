package com.edgestream.worker.runtime.task.broker;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.edgestream.worker.operator.Operator;
import com.edgestream.worker.runtime.reconfiguration.state.AtomicKey;
import com.edgestream.worker.runtime.reconfiguration.state.RoutingKey;
import com.edgestream.worker.runtime.reconfiguration.state.StateTransferPlan;
import com.edgestream.worker.runtime.task.model.TaskRequestSetupOperator;
import com.edgestream.worker.runtime.topology.Topology;
import com.edgestream.worker.runtime.topology.TopologyAttributeAddressManager;
import com.edgestream.worker.runtime.topology.TopologyOperatorAddressManager;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.core.management.QueueControl;



import java.io.IOException;
import java.util.*;

import static org.apache.activemq.artemis.api.core.RoutingType.ANYCAST;


public class TaskBrokerConfig {

    final boolean FALSE = false;
    final boolean TRUE = true;
    ActiveMQServerControl serverControl;
    MBeanServerConnection connection;
    String default_topology_address = "merlin_default";
    String taskManagerID;
    String broker_ip;


    /**
     *
     * @param broker_ip
     * @throws Exception
     */
    public TaskBrokerConfig(String broker_ip, String taskManagerID) throws Exception {


        this.broker_ip = broker_ip;
        this.setupActiveMQServerControlConnection();
        this.taskManagerID = taskManagerID;

    }


    /*************************************************************************************************************
     *
     *                                          Broker Controllers
     *
     * ***********************************************************************************************************/


    public ActiveMQServerControl getServerControl() {
        return serverControl;
    }

    public QueueControl getQueueController(String address, String queue) {

        ObjectName on = null;
        try {
            on = ObjectNameBuilder.create("org.apache.activemq.artemis","merlin01").getQueueObjectName(SimpleString.toSimpleString(address),SimpleString.toSimpleString(queue), ANYCAST);
        } catch (Exception e) {
            e.printStackTrace();
        }
        JMXConnector connector = null;
        try {
            connector = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + this.broker_ip + ":3000/jmxrmi"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        MBeanServerConnection mbsc = null;
        try {
            mbsc = connector.getMBeanServerConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }

        QueueControl queueControl = MBeanServerInvocationHandler.newProxyInstance(mbsc, on, QueueControl.class, false);

        return queueControl;
    }




    /**
     * This method is used to obtain the ActiveMQ address controller that you can use to invoke management operations on the this specific address
     * @param address
     * @return
     * @throws Exception
     */
    public AddressControl getAddressController(String address)  {

        AddressControl addressControl = null;
        try {
            ObjectName on = ObjectNameBuilder.create("org.apache.activemq.artemis", "merlin01").getAddressObjectName(SimpleString.toSimpleString(address));
            JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + this.broker_ip + ":3000/jmxrmi"));
            MBeanServerConnection mbsc = connector.getMBeanServerConnection();


            addressControl = MBeanServerInvocationHandler.newProxyInstance(mbsc, on, AddressControl.class, false);

            //System.out.println("Created a controller for address: " + addressControl.getAddress());
        }catch(Exception e){
            e.printStackTrace();

        }

        return addressControl;

    }



    public String getTaskManagerID() {
        return taskManagerID;
    }

    public String getBroker_ip() {
        return broker_ip;
    }




    public void addDivertToDefaultTopology(Topology topology)  {

        /** THIS SHOULD GET CALLED AFTER THE PRIMARY TOPOLOGY AND NO MATCH ADDRESS HAVE BEEN CREATED AND
         * ARE READY TO ACCEPT TUPLES OTHERWISE INCOMING TUPLES WILL GET LOST*/

        //2. Create a divert on the default address that will forward based on the topology ID of the tuple
        String topologyFilter = "(topologyID='" + topology.getTopologyID()+ "')";
        String divertName = topology.getTopologyID()+ "_default_divert";
        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Creating divert " + divertName + "_on_" + default_topology_address + " with the following filter: "+ topologyFilter);


        String name = divertName;
        String routing = "push_to_primary_topology_address";
        String address = this.default_topology_address; //the merlin_default address
        String forwardingAddress = topology.getPrimaryTopologyAddress();
        String filterString = topologyFilter;


        //1. Get the address controller for the default address and pause it
        //System.out.println("Pausing address: [" + default_topology_address + "]");
        //getAddressController(default_topology_address).pause();    ----------- removed Apr 4 2021
        try {
            //this divert is exclusive meaning, tuples that follow this divert do not get sent to the default any longer
            this.serverControl.createDivert(name,routing,address,forwardingAddress,TRUE,filterString,null, (String) null,"ANYCAST");
        } catch (Exception e) {
            e.printStackTrace();
        }


        //3. Unpause the default address. Tuples matching the new divert filter
        //getAddressController(default_topology_address).resume();----------- removed Apr 4 2021

        //4. TODO: add logic to remove diverts after the topology is removed

    }






    /****************************************************************************************************************
     *
     *
     *                                       New address features
     *
     *
     ****************************************************************************************************************/





    public String setupNewOperatorAddressAndQueue(Topology topology, TaskRequestSetupOperator taskRequestSetupOperator) throws Exception {
        System.out.println("**********************************************************************");
        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Attempting to install new EdgeStream operator onto topology: " + topology.getTopologyID());

        Operator operator = taskRequestSetupOperator.getOperator();

        //add the edgestream.operator to the topology list
        topology.installOperator(operator);

        //1. Setup operator address(if needed) - note: no queues are created on this address anymore, queues are now created on attribute addresses
        //an initial divert will be created
        TopologyOperatorAddressManager topologyOperatorAddressManager = setupOperatorAddress(topology,operator);
        String operatorAddress = topologyOperatorAddressManager.getAddress();

        //TODO: Operators will no longer be setup on the operator address, they will always belong to an attribute address
        //1b. Add operator address so it can be used later to shutdown the operators
        operator.setOperatorAddress(operatorAddress);


        /**************************************************************************
         * Operator Routing Setup
         ************************************************************************/

        String attributeQueueBinding = null;
        String attributeAddress = null;


        ArrayList<AtomicKey> keyList = new ArrayList<>();
        keyList.add(new AtomicKey("any","any"));

        RoutingKey routingKey = new RoutingKey(keyList);



        if(taskRequestSetupOperator.hasBootstrapState()) {
            StateTransferPlan stateTransferPlan = taskRequestSetupOperator.getStateTransferPlan();
            //We are only interested in the routing key for routing
            if (stateTransferPlan.stateTransferPlanContainsKey(RoutingKey.class.getName())) {
                routingKey = (RoutingKey) stateTransferPlan.getOperatorStateObject(RoutingKey.class.getName());
            }
         }




        //create a new attribute address (if needed)
        TopologyAttributeAddressManager topologyAttributeAddressManager = setupAttributeAddress(topology, operator, routingKey, taskRequestSetupOperator);
        attributeAddress = topologyAttributeAddressManager.getAttributeAddress();
        operator.setAttributeAddress(attributeAddress);

        //pause the attribute address
        //topologyAttributeAddressManager.pauseAttributeAddress();


        // Setup the new operator queue on attribute address
        // we always setup a new attribute queue for each operator instance
        // the queue is disabled at this point
        attributeQueueBinding = addNewAttributeQueue(topology, operator, routingKey);

        operator.setOperatorQueueName(attributeQueueBinding);

        if (!topologyAttributeAddressManager.isNewAttributeAddress()){
            //return early because the attribute address was previously created
            //and so we just added one worker to the queue so no need to do anything further
            return attributeAddress +"::"+ attributeQueueBinding;
        }


        topologyAttributeAddressManager.resumeAttributeAddress();


        /*******At this point operator and attribute addresses have been created. The attribute queue has also been created **********/


        /***************************************************
         * Divert reconfiguration beings here
        ****************************************************/

        //Operator address and queue are ready, now need to add divert filter on the topology address to send tuples here that match the operator tuple type

        /**Setup the divert to allow tuples to flow the new Operator queue on the primary topology address *********/
        //1. Pause the address to block incoming tuples
        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Pausing the following address: [" + topology.getPrimaryTopologyAddress() + "]");

        getAddressController(topology.getPrimaryTopologyAddress()).pause();

        //2. Destroy the old divert
        destroyCatchAllDivert(topology);

        //3. Add new operator filter and recompute the new primary divert filter string(this is used to create the filter
        // only for the no match address
        boolean divertAlreadyExists =topology.addOperatorDivertToTopologyAddress("(tupleType='" + operator.getInputType()+ "')");

        //4. ReCreate the divert for the no match address
        createDivertOnAddress(topology,topology.getPrimary_catch_all_divert_name(),"push_to_no_match_address",topology.getPrimary_catch_all_divert_filter_expression());


        //4.a. create divert on primary topology address to send tuples to new operator address
        if(!divertAlreadyExists) {
            createDivertFromTopologyAddressToOperatorAddress(topology, operator);
        }
        //4b. resume the operator address
        //getAddressController(operatorAddress).resume();
        topologyOperatorAddressManager.resumeOperatorAddress();



        //5. resume the primary topology address
        getAddressController(topology.getPrimaryTopologyAddress()).resume();



        //6. Now the queue is usable, so return it to the user
        return attributeAddress +"::"+ attributeQueueBinding; /** This is the address::queue where the edgestream.operator will read from. It is needed to setup the MessageConsumerClient*/
    }




    private TopologyOperatorAddressManager setupOperatorAddress(Topology topology, Operator operator) throws Exception {

        //create operator address for this specific operator type
        String operator_primary_topology_prefix = "topology_operator_pipeline_";
        String addressName = operator_primary_topology_prefix + topology.getTopologyID() + "_for_type_" + operator.getInputType();

        //first check to see if address has already been setup if so don't try to create it
        boolean addressExists = topology.operatorAddressExists(addressName);
        TopologyOperatorAddressManager topologyOperatorAddressManager = topology.getTopologyOperatorAddressManager(addressName);
        topologyOperatorAddressManager.setTupleType(operator.getInputType());

        if(!addressExists) {

            String operator_topology_pipeline_address = this.serverControl.createAddress(addressName, "ANYCAST");
            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Created operator topology address: " + operator_topology_pipeline_address);

            topologyOperatorAddressManager.setAddressController(getAddressController(addressName));

            topology.addOperatorAndAddress(operator,addressName);
            String divertName = topologyOperatorAddressManager.getAnyTupleTypesDivertName();
            String routingName = "route_from_" + addressName + "_to_" +  topology.getNoMatchTopologyAddress();
            String filter = ""; // divert everything
            createDefaultOperatorAddressToNoMatchAddressDivert(topology,topologyOperatorAddressManager, divertName,routingName,filter);

        }else{

            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Operator address: " + addressName + " already exists, will not create again");
            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Adding operator: " + operator.getOperatorID().getOperatorID_as_String() +" to existing address: " + addressName);
            topology.addOperatorAndAddress(operator,addressName);
        }

        //this will create a TopologyOperatorAddressManager if its the first time this operator address is being setup
        return topologyOperatorAddressManager;

    }


    public void createDefaultOperatorAddressToNoMatchAddressDivert(Topology topology, TopologyOperatorAddressManager topologyOperatorAddressManager, String divertName, String routingName, String filter) throws Exception {

        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Creating divert " +divertName + "_on"  + topologyOperatorAddressManager.getAddress() + " with the following filter: "+ filter);

        String routing = routingName;
        String sourceAddress =  topologyOperatorAddressManager.getAddress();
        String forwardingAddress = topology.getNoMatchTopologyAddress();
        String filterString = filter;

        this.serverControl.createDivert(divertName,routing,sourceAddress,forwardingAddress,TRUE,filterString,null, (String) null,"ANYCAST");
    }



    private TopologyAttributeAddressManager setupAttributeAddress(Topology topology, Operator operator, RoutingKey routingKey, TaskRequestSetupOperator taskRequestSetupOperator) {

        //create operator address for this specific operator type
        String operator_primary_topology_prefix = "topology_operator_pipeline_";
        String attributeAddress = operator_primary_topology_prefix + topology.getTopologyID() + "_for_type_" + operator.getInputType() +"_for_attribute_" + routingKey.routingKeyAttributeQueueName();

        //first check to see if address has already been setup if so don't try to create it
        String operatorAddress = topology.getOperatorAddress(operator);
        TopologyOperatorAddressManager topologyOperatorAddressManager = topology.getTopologyOperatorAddressManager(operatorAddress);

        // if the routing key exits then and attribute address exits
        boolean addressExists = topologyOperatorAddressManager.routingKeyExists(routingKey);

        TopologyAttributeAddressManager topologyAttributeAddressManager;
        if(!addressExists) {
            try {
                //Pause operator address so a new attribute address can be created if needed, and a operator queue can be created on that attribute address
                //getAddressController(operatorAddress).pause();
                topologyOperatorAddressManager.pauseOperatorAddress();
                this.serverControl.createAddress(attributeAddress, "ANYCAST");
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Created operator attribute address: " + attributeAddress);
            topologyOperatorAddressManager.addAttributeAddress(routingKey,operator,attributeAddress); //create the new attribute address manager
            topologyAttributeAddressManager = topologyOperatorAddressManager.getTopologyAttributeAddressManager(routingKey); // get the newly created attribute address manager
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            AddressControl addressControl = getAddressController(attributeAddress);

            try {
                System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Pausing address: [" + attributeAddress + "]");
                addressControl.pause();
            } catch (Exception e) {
                e.printStackTrace();
            }
            topologyAttributeAddressManager.setAddressController(addressControl); // this sets a live controller for this specific address
            topologyAttributeAddressManager.pauseAttributeAddress();


            topologyAttributeAddressManager.addOperator(operator);
            updateOperatorAddressDiverts(topology, topologyOperatorAddressManager);

        }else{

            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Attribute address [" + attributeAddress + "] already exists, will not create again");
            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Adding operator: [" + operator.getOperatorID().getOperatorID_as_String() +"] to existing address: " + attributeAddress);
            //we can use this reference to find out which attribute address the operator is on.
            topologyAttributeAddressManager = topologyOperatorAddressManager.getTopologyAttributeAddressManager(routingKey);
            topologyAttributeAddressManager.addOperator(operator);
        }

        return topologyAttributeAddressManager;

    }

    private void updateOperatorAddressDiverts(Topology topology ,TopologyOperatorAddressManager topologyOperatorAddressManager){
        System.out.println(System.currentTimeMillis() + " Updating divert filters....");


        /**
         * This function expects that the state of the TopologyOperatorAddressManager has already been updated.
         *
         * Logic:
         * #1
         * if no operator is installed, then the divert on the operator address needs to forward to:
         *  divert 1 forwardingAddress = topology.getNoMatchTopologyAddress()
         *
         * #2
         * if and operator of type "any" is installed, the the divert on the operator address needs to forward to:
         *  divert 1 forwardingAddress = topology.getAttributeAddress(operator,routingKey);
         *
         * #3
         * if an operator of type "any" is installed and there are other operators installed:
         *  divert 1 forwardingAddress = topology.getAttributeAddress(operator,any) ;
         *           filter  =  ActiveMQOperatorAddressFilterExpression;
         *  divert n forwardingAddress = topology.getAttributeAddress(operator, n)
         *
         * #4
         * if a no any operator is installed, but there is an operator of some attribute type installed:
         *  divert1 forwardingAddress =  topology.getAttributeAddress(operator,routingKey)
         *  divert2 forwardingAddress =  topology.getNoMatchTopologyAddress()
         *
         *
         */




        /** #2 *****This should be called when the operator address has no existing Attribute addresses and we want to install a new ANY operator *************************/
        if(topologyOperatorAddressManager.hasActiveAnyOperator() && topologyOperatorAddressManager.getInstalledAttributeDiverts().isEmpty()){
            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: -----------------------------------------------------------------Using option #2");
            topologyOperatorAddressManager.deactivateForwardToNoMatch();


            //2. Remove the forward to no match divert
            destroyDivert(topologyOperatorAddressManager.getAnyTupleTypesDivertName());

            //3. Add the any operator divert
            String sourceAddress = topologyOperatorAddressManager.getAddress();  // this never changes
            ArrayList<AtomicKey> keyList = new ArrayList<>();
            keyList.add(new AtomicKey("any","any"));
            RoutingKey anyRoutingKey = new RoutingKey(keyList);


            TopologyAttributeAddressManager topologyAttributeAddressManager = topologyOperatorAddressManager.getTopologyAttributeAddressManager(anyRoutingKey);
            String forwardingAddress = topologyAttributeAddressManager.getAttributeAddress();

            String filterString ="";
            String divertName = topologyOperatorAddressManager.getAnyTupleTypesDivertName();

            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Creating divert [" + divertName + "] on ["  + sourceAddress + "] with the following filter: ["+ filterString +"]");

            String routingName = "forward_tuples_to_attribute_address_" + forwardingAddress;

            try {
                this.serverControl.createDivert(divertName,routingName,sourceAddress,forwardingAddress,TRUE,filterString,null, (String) null,"ANYCAST");
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        /** #3 *****At least one keyed-operator is installed and there is also an any operator installed *************************/
        if(topologyOperatorAddressManager.hasActiveAnyOperator() && !topologyOperatorAddressManager.getInstalledAttributeDiverts().isEmpty()){
            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: -----------------------------------------------------------------Using option #3");

            //1. Deactivate the forward to the no match
            topologyOperatorAddressManager.deactivateForwardToNoMatch();

            //2. Removal all diverts
            for(String divertNames : topologyOperatorAddressManager.getInstalledAttributeDiverts()){
                destroyDivert(divertNames);
            }

            //3. Remove the forward to any tuple divert
            destroyDivert(topologyOperatorAddressManager.getAnyTupleTypesDivertName());


            //3b. add the any tuple divert to forward to the any operator address
            try{
                ArrayList<AtomicKey> keyList = new ArrayList<>();
                keyList.add(new AtomicKey("any","any"));
                RoutingKey anyRoutingKey = new RoutingKey(keyList);

                TopologyAttributeAddressManager topologyAttributeAddressManager = topologyOperatorAddressManager.getTopologyAttributeAddressManager(anyRoutingKey);
                //topologyOperatorAddressManager.installAttributeDivert(topologyAttributeAddressManager.getDivertName());

                String sourceAddress = topologyOperatorAddressManager.getAddress();  // this never changes
                String forwardingAddress = topologyAttributeAddressManager.getAttributeAddress();
                String filterString = topologyOperatorAddressManager.getAnyOperatorFilterExpression();
                String divertName = topologyAttributeAddressManager.getDivertName();
                System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Creating divert [" + divertName + "] on ["  + sourceAddress + "] with the following filter: ["+ filterString+"]");
                String routingName = "forward_tuples_to_attribute_address_" + forwardingAddress;

                try {
                    this.serverControl.createDivert(divertName,routingName,sourceAddress,forwardingAddress,TRUE,filterString,null, (String) null,"ANYCAST");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }catch (Exception e) {
                e.printStackTrace();
            }




            //4. Add all the diverts for the installed operators
            try {
                Map mp = topologyOperatorAddressManager.getRoutingKeyToAddressMapping();

                Iterator it = mp.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry) it.next();

                    /**Create the diverts***/

                    String sourceAddress = topologyOperatorAddressManager.getAddress();  // this never changes
                    RoutingKey rk = (RoutingKey) pair.getKey();
                    TopologyAttributeAddressManager topologyAttributeAddressManager = topologyOperatorAddressManager.getTopologyAttributeAddressManager(rk);
                    String forwardingAddress = topologyAttributeAddressManager.getAttributeAddress();
                    String filterString = topologyAttributeAddressManager.getFilterString();
                    String divertName = topologyAttributeAddressManager.getDivertName();
                    System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Creating divert: [" + divertName + "] on [" + sourceAddress + "] with the following filter: [" + filterString+"]");
                    String routingName = topologyAttributeAddressManager.getRoutingName();

                    try {
                        this.serverControl.createDivert(divertName, routingName, sourceAddress, forwardingAddress, TRUE, filterString, null, (String) null, "ANYCAST");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }


                    /***********************/

                }
            }catch (Exception e) {
                e.printStackTrace();
            }



        }

        /** #4 ***At least one keyed-operator is to be installed, but no any operator is installed *************************/
        if(!topologyOperatorAddressManager.hasActiveAnyOperator() && !topologyOperatorAddressManager.getInstalledAttributeDiverts().isEmpty()){
            System.out.println("[TaskBrokerConfig]: -----------------------------------------------------------------Using option #4");
            //1. Activate the forward to the no match
            topologyOperatorAddressManager.activateForwardToNoMatch();

            //2. Removal all diverts
            for(String divertNames : topologyOperatorAddressManager.getInstalledAttributeDiverts()){
                destroyDivert(divertNames);
            }

            //3. Remove the forward to any tuple divert
            destroyDivert(topologyOperatorAddressManager.getAnyTupleTypesDivertName());


            //3b. add the any tuple divert to forward to the topology no match address
            try{
                String sourceAddress = topologyOperatorAddressManager.getAddress();  // this is always the operator address
                String forwardingAddress = topology.getNoMatchTopologyAddress(); //sending to topology no match address
                String filterString = topologyOperatorAddressManager.getAnyOperatorFilterExpression();
                String divertName = topologyOperatorAddressManager.getAnyTupleTypesDivertName();
                System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Creating divert: [" + divertName + "] on [" + sourceAddress + "] with the following filter: ["+ filterString+"]");
                String routingName = "forward_tuples_to_no_match_address_" + forwardingAddress;

                try {
                    this.serverControl.createDivert(divertName,routingName,sourceAddress,forwardingAddress,TRUE,filterString,null, (String) null,"ANYCAST");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }catch (Exception e) {
                e.printStackTrace();
            }




            //4. Add all the diverts for the installed operators
            try {
                Map mp = topologyOperatorAddressManager.getRoutingKeyToAddressMapping();

                Iterator it = mp.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry) it.next();

                    /**Create the attribute diverts***/

                    RoutingKey rk = (RoutingKey) pair.getKey();
                    TopologyAttributeAddressManager topologyAttributeAddressManager = topologyOperatorAddressManager.getTopologyAttributeAddressManager(rk);
                    try {
                        String sourceAddress = topologyOperatorAddressManager.getAddress();  // this never changes
                        String forwardingAddress = topologyAttributeAddressManager.getAttributeAddress();
                        String filterString = topologyAttributeAddressManager.getFilterString();
                        String divertName = topologyAttributeAddressManager.getDivertName();
                        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Creating divert: [" + divertName + "] on [" + sourceAddress + "] with the following filter: [" + filterString +"]");
                        String routingName = topologyAttributeAddressManager.getRoutingName();
                        this.serverControl.createDivert(divertName, routingName, sourceAddress, forwardingAddress, TRUE, filterString, null, (String) null, "ANYCAST");

                    } catch (Exception e) {
                        e.printStackTrace();
                    }


                    /**Create the dual routing diverts *********************/


                    //this operator needs to be dual routed, meaning we need to inject a tuple and send the data up AND to this new attribute address
                   //if(rk.hasSyncTuple() && topologyAttributeAddressManager.hasDualRoutingEnabled()){
                    if(topologyAttributeAddressManager.hasDualRoutingEnabled()){
                        //TODO: verify if we should create multiple diverts for each key or have a single divert with all the keys
                        try {
                            String sourceAddress = topologyAttributeAddressManager.getAttributeAddress();
                            String forwardingAddress = topology.getNoMatchTopologyAddress(); //sending to topology no match address
                            String filterString = ""; //  no filter means forward a copy of everything
                            String divertName = topologyAttributeAddressManager.getDualRouteDivertName();
                            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Creating dual route divert: [" + divertName + " on "  + sourceAddress + "] with the following filter: [" + filterString +"]");
                            String routingName = "forward_tuples_to_no_match_address_" + forwardingAddress;

                            //destroy if the divert exists TODO: in the future change this so we arent destroying something that is already there
                            destroyDivert(divertName);

                            //recreate
                            this.serverControl.createDivert(divertName,routingName,sourceAddress,forwardingAddress,FALSE,filterString,null, (String) null,"ANYCAST");

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        //}
                    }
                }//end of while loop
            }catch (Exception e) {
                e.printStackTrace();
            }
        }//end of step #4




        /** #1 *****When no operator is installed ********this is placed last to avoid execution of step 2 right after************************/
        if(topologyOperatorAddressManager.isForwardToNoMatchActive() && topologyOperatorAddressManager.getInstalledAttributeDiverts().isEmpty()){
            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: -----------------------------------------------------------------Using option #1");

            //Add the no match divert
            String sourceAddress = topologyOperatorAddressManager.getAddress();  // this never changes
            String forwardingAddress = topology.getNoMatchTopologyAddress();

            String filterString =  "";
            String divertName = topologyOperatorAddressManager.getAnyTupleTypesDivertName();

            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Creating no match divert " + divertName + " on "  + sourceAddress + " with the following filter: "+ filterString);

            String routingName = "forward_tuples_to_no_match_for_type_" + topologyOperatorAddressManager.getTupleType();

            try {
                this.serverControl.createDivert(divertName,routingName,sourceAddress,forwardingAddress,TRUE,filterString,null, (String) null,"ANYCAST");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }



    }



    /**
     * TODO: For now this supports only a single routing key. We should add multiple key support so we can filter on multiple attributes
     * @param topology
     * @param operator
     * @param routingKey
     * @return
     * @throws Exception
     */
    private String addNewAttributeQueue(Topology topology, Operator operator, RoutingKey routingKey) throws Exception {

        Boolean is_durable = FALSE;
        String queueType = "ANYCAST";
        String new_attribute_queue_name = topology.getTopologyID()
                                + "_" +operator.getOperatorID().getOperatorID_as_String()
                                +"_input_queue_"
                                + operator.getInputType()
                                + routingKey.routingKeyAttributeQueueName();

        String operatorAddress = topology.getOperatorAddress(operator);  // this never changes
        TopologyOperatorAddressManager topologyOperatorAddressManager = topology.getTopologyOperatorAddressManager(operatorAddress);
        TopologyAttributeAddressManager topologyAttributeAddressManager = topologyOperatorAddressManager.getTopologyAttributeAddressManager(routingKey);
        String attributeAddress = topologyAttributeAddressManager.getAttributeAddress();

        // TODO: roll back this change once the warmup feature is complete
        if(!topologyAttributeAddressManager.getAttributeQueueName().equalsIgnoreCase("NOT_SET")){
            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Attribute queue [" + new_attribute_queue_name  + " ] already exists, will not create");
            new_attribute_queue_name = topologyAttributeAddressManager.getAttributeQueueName();
        }else{

            this.serverControl.createQueue(attributeAddress, queueType, new_attribute_queue_name, null, is_durable, 10, false, false);
            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Created attribute queue: " + new_attribute_queue_name + " on address " + topology.getOperatorAddress(operator));
            topologyAttributeAddressManager.setAttributeQueueName(new_attribute_queue_name);
        }


        //this.serverControl.createQueue(attributeAddress, queueType, new_attribute_queue_name, null, is_durable, 10, false, false);
        //System.out.println("Created attribute queue: " + new_attribute_queue_name + " on address " + topology.getOperatorAddress(operator));

        return  new_attribute_queue_name;

    }




    public void createDivertFromAttributeAddressToNoMatchAddress(TopologyAttributeAddressManager topologyAttributeAddressManager)  {


        String filterString =  "" ; //divert everything
        String divertName = topologyAttributeAddressManager.getDualRouteDivertName() ;
        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Creating attribute divert [" + divertName + "] on ["  + topologyAttributeAddressManager.getAttributeAddress() + "] with the following filter: [" + filterString + "]");

        String name = divertName;
        String sourceAddress = topologyAttributeAddressManager.getAttributeAddress();
        String forwardingAddress = topologyAttributeAddressManager.getBoundTopologyOperatorAddressManager().getBoundTopology().getNoMatchTopologyAddress();
        String routingName = "dual_route_tuples_to_" + forwardingAddress;

        try {
            this.serverControl.createDivert(name,routingName,sourceAddress,forwardingAddress,FALSE,filterString,null, (String) null,"ANYCAST");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }







    private void createDivertFromTopologyAddressToOperatorAddress(Topology topology, Operator operator) throws Exception {


        String filterString =  "(tupleType='" + operator.getInputType()+ "')" ;
        String divertName = topology.getTopologyID() + "_" + operator.getInputType() + "_divert";
        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Creating operator divert " + divertName + " on "  + topology.getPrimaryTopologyAddress() + " with the following filter: "+ filterString);

        String name = divertName;
        String routingName = "route_tuples_to_operator_queue_for_type_" + operator.getInputType();
        String sourceAddress = topology.getPrimaryTopologyAddress();
        String forwardingAddress = topology.getOperatorAddress(operator);

        this.serverControl.createDivert(name,routingName,sourceAddress,forwardingAddress,TRUE,filterString,null, (String) null,"ANYCAST");
    }


    public void destroyDivert(String divertName) {



         String[] divertNames = this.serverControl.getDivertNames();

         boolean divertExists = false;
         for(String str : divertNames){
             if(str.equalsIgnoreCase(divertName)){

                 divertExists = true;
             }

         }


        if(divertExists) {
            try {
                System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Destroying divert [" + divertName  + "].....");
                this.serverControl.destroyDivert(divertName);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else{

            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: The following divert does not exist:[" + divertName + "]");
        }

    }




    /**
     * This function sets up the topology bridge from a broker to the parent broker.
     * At the moment only one broker for each data center is expected and it is how two data centers move messages
    */
    public void setUpTopologyBridge(Topology topology , int id){

        String bridgeSourceQueue = topology.getNoMatchForwardingQueueFQQN(); //Source is the single catchall queue on this broker for this topology

        //Forwarding address is the PrimaryTopologyAddress on the parent broker. Parents are always assumed to be running the topology because parents get created first.
        //String bridgeForwardingAddress = topology.getPrimaryTopologyAddress();

        //If we are not sure that the parent will be running the topology always send to the parent

        String bridgeForwardingAddress = "merlin_default";



        String bridge_name = topology.getTopologyID() + "_bridge_" + id;
        String connector_name = "parent_broker"; //this must match the connector name in the broker.xml

        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Trying to create bridge: "+ bridge_name);
        try {
            this.serverControl.createBridge(bridge_name,bridgeSourceQueue,bridgeForwardingAddress,null,null,1000,2,3,3,TRUE,100000,30000, connector_name,FALSE,FALSE,null,null);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }




    /**
     *
     * @param topology
     * @throws Exception
     */
    public void destroyCatchAllDivert(Topology topology) throws Exception {

        this.serverControl.destroyDivert(topology.getPrimary_catch_all_divert_name());

    }


    /**
     *
     *
     * @throws IOException
     * @throws MalformedObjectNameException
     */
    void setupActiveMQServerControlConnection() throws IOException, MalformedObjectNameException {

        //Setup the mBean server that we will use for all operations
        JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + this.broker_ip + ":3000/jmxrmi"));
        connector.connect();
        connection = connector.getMBeanServerConnection();

        String beanName = "org.apache.activemq.artemis:broker=" + "\"merlin01\"";
        ObjectName server_mbeanName = new ObjectName(beanName);
        this.serverControl = MBeanServerInvocationHandler.newProxyInstance(connection, server_mbeanName, ActiveMQServerControl.class, true);


    }




    public void addTopologyLocalMetricsAddressAndQueue(String localMetricsAddress, String metricsQueue){

        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Attempting to create local operator metrics address..");
        try {
            String topology_metrics_address_result = this.serverControl.createAddress(localMetricsAddress,"ANYCAST");
            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Created local topology metrics address: " + topology_metrics_address_result);
        } catch (Exception e) {
            e.printStackTrace();
        }


        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Attempting to create local metrics queue..");
        String queueFilter = "";
        Boolean is_durable = FALSE;
        String queueType = "ANYCAST";

        try {
            this.serverControl.createQueue(localMetricsAddress, metricsQueue, queueFilter, is_durable ,queueType);
            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Created topology local metrics queue: " + metricsQueue + " on address " + localMetricsAddress) ;

        } catch (Exception e) {
            e.printStackTrace();
        }

    }




    public void addTopologyManagementAddressAndQueue(String managementAddress, String managementQueue){



        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Attempting to create [TM] to [OP] management address..");
        try {
            String topology_management_address_result = this.serverControl.createAddress(managementAddress,"ANYCAST");
            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Created topology management address: " + topology_management_address_result);
        } catch (Exception e) {
            e.printStackTrace();
        }


        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Attempting to create  [TM] to [OP] management queue..");
        String queueFilter = "";
        Boolean is_durable = FALSE;
        String queueType = "ANYCAST";

        try {
            this.serverControl.createQueue(managementAddress, managementQueue, queueFilter, is_durable ,queueType);
            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Created topology management queue: " + managementQueue + " on address " + managementAddress) ;

        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    public void addTopologyOpToOpManagementAddressAndQueue(String taskManagerToOperatorManagementAddress, String taskManagerToOperatorManagementQueue) {

        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Attempting to create [OP] to [OP] management address..");
        try {
            String topology_management_address_result = this.serverControl.createAddress(taskManagerToOperatorManagementAddress,"ANYCAST");
            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Created topology management address: " + topology_management_address_result);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Attempting to create [OP] to [OP] management queue..");
        String queueFilter = "";
        Boolean is_durable = FALSE;
        String queueType = "MULTICAST";

        try {
            this.serverControl.createQueue(taskManagerToOperatorManagementAddress, taskManagerToOperatorManagementQueue, queueFilter, is_durable ,queueType);
            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Created topology management queue: " + taskManagerToOperatorManagementQueue + " on address " + taskManagerToOperatorManagementAddress) ;

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     *
     * @param topology
     * @throws Exception
     */
    public void addTopologyAddress(Topology topology) throws Exception {


        //create primary address for this specific topology
        String primary_topology_prefix = "primary_topology_pipeline_";
        String addressName = primary_topology_prefix + topology.getTopologyID();

        String topology_pipeline_address = this.serverControl.createAddress(addressName,"ANYCAST");
        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Created Primary topology pipeline: " + topology_pipeline_address);

        topology.setPrimaryTopologyAddress(addressName);



        //create no match address for this specific topology
        String no_match_topology_prefix = "no_match_topology_pipeline_";
        String noMatchAddress = no_match_topology_prefix + topology.getTopologyID();
//        String noMatchAddress = "merlin_default";
        topology.setNoMatchTopologyAddress(noMatchAddress);

        // This check is meant for moving very high number of keys
        // In this case, large no. of bridges are needed to move tuples from no_match_topology
        // Creating bridges take time, so we create the address, queue and bridges during broker start
        // Can uncomment these parts when dealing with other cases
        String[] addresses = this.serverControl.getAddressNames();
        if (!Arrays.asList(addresses).contains(noMatchAddress)) {
//            String no_match_topology_pipeline_address = this.serverControl.createAddress(noMatchAddress,"ANYCAST");
//            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Created No Match topology pipeline: " + no_match_topology_pipeline_address);

            //create the forwarding queue on the no match address
            this.addNewForwardingQueue(topology);
        }

        //create a divert that initially diverts everything to the no match address until an edgestream.operator is installed.
        //when at least one edgestream.operator gets installed this divert will need to be updated
        String filter = "null"; //null because we want everything diverted TODO: investigate to see if this filter is even needed. Maybe an empty filter is needed to match all

        this.createDivertOnAddress(topology, topology.getPrimary_catch_all_divert_name(), "push_to_no_match_address",filter);

        //set the topology to empty because this method always gets called before any operators are setup
        topology.setEmptyTopology(true);



        /**
         * Since all producers for this specific topology were sending tuples to the default address we need to add a divert to that address
         * so that tuples with the attribute matching this topologyID will get diverted to this primaryTopologyAddress
         *
         * Change: June 23, 2020, the default topology should get added/updated after the operator has been started, to avoid the tuples just queueing while the operator is initializing
         * ***/
         //this.addDivertToDefaultTopology(topology);
    }


    /**
     *
     * @param topology
     * @param divertName
     * @param routingName
     * @param filter
     * @throws Exception
     */
    public void createDivertOnAddress(Topology topology , String divertName, String routingName, String filter) throws Exception {

        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Creating divert " +divertName + "_on"  + topology.getPrimaryTopologyAddress() + " with the following filter: "+ filter);

        //When a operator is added we create a divert on this address ex NOT (tupleType='A')
        // This way if its not A type it will diverted to the forwarding queue
        String name = divertName;
        String routing = routingName;
        String address = topology.getPrimaryTopologyAddress();
        String forwardingAddress = topology.getNoMatchTopologyAddress();
        String filterString = filter;


        this.serverControl.createDivert(name,routing,address,forwardingAddress,TRUE,filterString,null, (String) null,"ANYCAST");
    }




    /**
     *
     * @param topology
     * @throws Exception
     */
    public void addNewForwardingQueue(Topology topology) throws Exception {

        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Attempting to create no match queue..");
        Topology tpg = topology;
        //1. create the new queue
        String queueFilter = "";
        Boolean is_durable = FALSE;
        String queueType = "ANYCAST";
        String new_queue_name = topology.getTopologyID() + "_to_be_forwarded";

//        this.serverControl.createQueue(tpg.getNoMatchTopologyAddress(), new_queue_name, queueFilter, is_durable ,queueType);
        topology.setNoMatchTopologyAddress_forwarding_queue(new_queue_name);

        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Created no match queue: " + new_queue_name + " on address " + topology.getNoMatchTopologyAddress()) ;

    }


    public void purgeQueue(String address, String queue){
        System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Attempting to purge queue.." + queue);

        QueueControl queueController = getQueueController(address, queue);
        try {
            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Warmup messages still in the queue" + queueController.getMessageCount());

            System.out.println(System.currentTimeMillis() + " [TaskBrokerConfig]: Calling the removeAllMessages method");
            queueController.removeAllMessages();
        } catch (Exception e) {
            e.printStackTrace();
        }



    }






    /***********************************************************************
                                        FOR DEBUG
     *********************************************************************/


    /**
     * A helper method to lookup all the Mbeans on the server incase you need to search for one to manage.
     * This method is not actively used during normal operation.
     * @throws IOException
     */
    void printListOfMBeans() throws IOException {

        Set<ObjectInstance> mBeans = this.connection.queryMBeans(null, null);

        Iterator<ObjectInstance> iterator = mBeans.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next().getObjectName());
        }

    }

    /**
     * A helper method to lookup all the high level domains on the server in case you need to manager other aspects
     * of the mbean server. This method is not actively used during normal operation.
     * @throws IOException
     */
    void printListOfJMXDomains() throws IOException {

        String[] domainsList = connection.getDomains();

        for (int i = 0; i < domainsList.length; i++) {

            System.out.println(domainsList[i]);
        }

    }



}