package com.edgestream.worker.runtime.topology;

import com.edgestream.worker.operator.Operator;
import com.edgestream.worker.runtime.reconfiguration.state.RoutingKey;
import com.edgestream.worker.runtime.task.TaskDriver;
import com.edgestream.worker.runtime.task.TaskMigrationManager;
import com.edgestream.worker.runtime.task.TaskWarmerManager;
import com.edgestream.worker.runtime.task.broker.TaskBrokerConfig;
import com.edgestream.worker.runtime.topology.exception.TopologyException;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Topology {

    private final String topologyID;
    private String PrimaryTopologyAddress;
    private final String metricsAddress;
    private String NoMatchTopologyAddress;
    private String NoMatchTopologyAddress_forwarding_queue;

    private String topologyJarFileName;

    private boolean isTopologyJarBuilt = false;

    private boolean isEmptyTopology;
    private final String primary_catch_all_divert_name;
    private String primary_catch_all_divert_filter_expression;
    private final ArrayList<Operator> installedOperators = new ArrayList<>();
    private final ArrayList<String> installedOperatorDiverts = new ArrayList<>();
    private TopologyState topologyState = TopologyState.PAUSED;
    private final TopologyReconfigurationManager topologyReconfigurationManager;
    private final ArrayList<Pair<Operator,String>>  operatorAndAddressList = new ArrayList<>();
    private final HashMap<String, TopologyOperatorAddressManager> topologyOperatorAddressManagers = new HashMap<>();
    private final TaskMigrationManager taskMigrationManager;
    private final TaskWarmerManager taskWarmerManager;
    private final TaskDriver boundTaskDriver;

    //[TaskManger] -> [Operator Container] Management Channel
    private final TopologyManagementListener topologyManagementListener;
    private final String taskManagerToOperatorManagementAddress;
    private final String taskManagerToOperatorManagementQueue;
    private final String taskManagerToOperatorManagementFQQN;

    //[Operator Container] -> [Operator Container] Management Channel
    private final String operatorToOperatorManagementAddress;
    private final String operatorToOperatorManagementQueue;
    private final String operatorToOperatorManagementFQQN;



    private final String operatorMetricsAddress;
    private final String operatorMetricsQueue;
    private final String operatorMetricsFQQN;


    public Topology(String topologyID, TaskDriver taskDriver){

        this.topologyID = topologyID;
        this.primary_catch_all_divert_name = topologyID + "_primary_divert";
        this.metricsAddress = taskDriver.getDefaultMetricsAddress();
        this.boundTaskDriver = taskDriver;
        this.taskMigrationManager = new TaskMigrationManager(this);
        this.topologyReconfigurationManager = new TopologyReconfigurationManager(this);
        this.taskWarmerManager = new TaskWarmerManager(this);

        String localBrokerIP = getBoundTaskDriver().getTaskBrokerConfig().getBroker_ip();

        /***************************************************************************************************************
         *
         *      Management Channel setup
         *
         *      Phase 1: this channel is an Artemis queue that the containerized operators will send
         *      management messages to the [Task Manager].
         *
         *      Phase 2: this channel is an Artemis queue that the containerized operators will send
         *      management messages to other [Operator Containers]
         *
         **************************************************************************************************************/

        //Phase 1a: Create the operator management queue and address
        taskManagerToOperatorManagementAddress = getTopologyID() + "_" + "tm_to_operator_management";
        taskManagerToOperatorManagementQueue = getTopologyID() + "tm_to_operator_incoming_messages";
        taskManagerToOperatorManagementFQQN =  taskManagerToOperatorManagementAddress + "::" + taskManagerToOperatorManagementQueue;
        boundTaskDriver.getTaskBrokerConfig().addTopologyManagementAddressAndQueue(taskManagerToOperatorManagementAddress,taskManagerToOperatorManagementQueue);

        //Phase 1b: Start the listener on the new operator management address
        topologyManagementListener = new TopologyManagementListener(taskManagerToOperatorManagementFQQN,localBrokerIP,this);
        topologyManagementListener.start();

        //Phase 2a: Create the operator management channel
        operatorToOperatorManagementAddress = getTopologyID() + "_" + "operator_to_operator_management";
        operatorToOperatorManagementQueue = getTopologyID() + "operator_to_operator_incoming_messages";
        operatorToOperatorManagementFQQN =  operatorToOperatorManagementAddress + "::" + operatorToOperatorManagementQueue;
        boundTaskDriver.getTaskBrokerConfig().addTopologyOpToOpManagementAddressAndQueue(operatorToOperatorManagementAddress,operatorToOperatorManagementQueue);



        /***************************************************************************************************************
         *
         *      Local Metrics Channel setup - this channel is a Artemis queue that the
         *      containerized operators will receive metrics messages from
         *
         **************************************************************************************************************/


        this.operatorMetricsAddress =  getTopologyID() + "_" + "operator_metrics";
        this.operatorMetricsQueue = getTopologyID() + "outgoing_metrics";
        this.operatorMetricsFQQN = operatorMetricsAddress + "::" + operatorMetricsQueue;

        //Phase 1: Create the operator metrics queue and address. The operator containers will subscribe to this queue
        this.boundTaskDriver.getTaskBrokerConfig().addTopologyLocalMetricsAddressAndQueue(operatorMetricsAddress,operatorMetricsQueue);


    }


    public TaskWarmerManager getTaskWarmerManager() {
        return taskWarmerManager;
    }

    public String getOperatorMetricsFQQN() {
        return operatorMetricsFQQN;
    }

    public String getTMToOperatorManagementFQQN() {
        return taskManagerToOperatorManagementFQQN;
    }

    public String getOperatorToOperatorManagementFQQN() {
        return operatorToOperatorManagementFQQN;
    }

    public TaskDriver getBoundTaskDriver(){

        return this.boundTaskDriver;
    }



    public String getTopologyID() {
        return topologyID;
    }

    public TaskMigrationManager getTaskMigrationManager() {
        return taskMigrationManager;
    }



/****************************************************************************************************************
     *
     *                                             Divert Controllers
     *
     ****************************************************************************************************************/
    /****************************************************************************************************************
     *                                             Topology divert controllers
     ****************************************************************************************************************/


    public boolean addOperatorDivertToTopologyAddress(String operatorFilter){

        //0. Check first if operator divert already exists

        boolean foundDivert = false;

        for(String operatorDivert: this.installedOperatorDiverts){
            if(operatorDivert.equalsIgnoreCase(operatorFilter)){
                System.out.println("Divert with filter: "+ operatorFilter  + " already exists, will not create.");
                foundDivert = true;
                break;
            }
        }


        if (!foundDivert){
            //1.add the new operator filter
            installedOperatorDiverts.add(operatorFilter);
            //2. rebuild the string filter after adding the new operator
            rebuildPrimary_catch_all_divert_filter_expression();
        }


        return foundDivert;



    }


    public void removeOperatorDivertFromTopologyAddress(String operatorFilter){

        //1.remove the new operator filter
        installedOperatorDiverts.remove(operatorFilter);

        //2. rebuild the string filter after adding the new operator
        rebuildPrimary_catch_all_divert_filter_expression();

    }


    private void rebuildPrimary_catch_all_divert_filter_expression(){


        String filter_string = null;
        for(String installedDiverts : installedOperatorDiverts){

            //1. The first filter does not have AND preceding it, so just all the filter string
            if(filter_string == null) {
                filter_string = "NOT ";
                filter_string = filter_string + installedDiverts;

            }else{
                filter_string = filter_string + " AND NOT " + installedDiverts;

            }
        }

        this.primary_catch_all_divert_filter_expression = filter_string;
        System.out.println("New primary address divert filter: " +primary_catch_all_divert_filter_expression );
    }
    /****************************************************************************************************************
     *                                           Operator Address Managers
     ****************************************************************************************************************/

    private boolean topologyOperatorAddressManagerExists(String operatorAddress){

        boolean operatorAddressManagerExists = this.topologyOperatorAddressManagers.containsKey(operatorAddress);

        return operatorAddressManagerExists;
    }

    public TopologyOperatorAddressManager getTopologyOperatorAddressManager(String operatorAddress){

        TopologyOperatorAddressManager topologyOperatorAddressManager = null;
        if(topologyOperatorAddressManagerExists(operatorAddress)) {
            topologyOperatorAddressManager = this.topologyOperatorAddressManagers.get(operatorAddress);
        }else{
            this.topologyOperatorAddressManagers.put(operatorAddress, new TopologyOperatorAddressManager(operatorAddress,this));
            topologyOperatorAddressManager = this.topologyOperatorAddressManagers.get(operatorAddress);
        }
        return topologyOperatorAddressManager;
    }


    /**
     * TODO: add logic here to check to see if addresses are already running, if so don't try to resume them again.
     * */
    public void resumeAllOperatorAndAttributeAddresses(){

        Iterator it = this.topologyOperatorAddressManagers.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            TopologyOperatorAddressManager topologyOperatorAddressManager = (TopologyOperatorAddressManager)pair.getValue();

            Iterator attributeIterator = topologyOperatorAddressManager.getRoutingKeyToAddressMapping().entrySet().iterator();
            while (attributeIterator.hasNext()) {

                Map.Entry innerPair = (Map.Entry) attributeIterator.next();
                TopologyAttributeAddressManager topologyAttributeAddressManager = (TopologyAttributeAddressManager)innerPair.getValue();
                topologyAttributeAddressManager.resumeAttributeAddress();
            }

        }

    }



    /****************************************************************************************************************
     *
     *                                             Topology State Management
     *
     ****************************************************************************************************************/

    public TopologyState getTopologyState() {
        return topologyState;
    }

    public void setTopologyState(TopologyState topologyState) {
        this.topologyState = topologyState;
    }

    public boolean isEmptyTopology() {
        return isEmptyTopology;
    }

    public void setEmptyTopology(boolean emptyTopology) {
        isEmptyTopology = emptyTopology;
    }

    public TopologyReconfigurationManager getTopologyReconfigurationManager() {
        return topologyReconfigurationManager;
    }


    /****************************************************************************************************************
     *
     *                                        Docker and jar file management
     *
     ****************************************************************************************************************/


    public String getTopologyJarFileName() {
        return topologyJarFileName;
    }

    public void setTopologyJarFileName(String topologyJarFileName) {
        this.topologyJarFileName = topologyJarFileName;
    }

    public boolean isTopologyJarBuilt(){
        return this.isTopologyJarBuilt;
     }

    public void setTopologyJarToBuilt(boolean status){
        this.isTopologyJarBuilt = status;
    }






    /****************************************************************************************************************
     *
     *                              Address Management
     *
     ****************************************************************************************************************/
    /****************************************************************************************************************
     *                              Default Address Management
     ****************************************************************************************************************/

                /**** This is managed by {@link TaskBrokerConfig}******/



    /****************************************************************************************************************
     *                              Primary Topology Address Management
     ****************************************************************************************************************/

    /**
     * This gets set by {@link TaskBrokerConfig#addTopologyAddress(com.edgestream.worker.runtime.topology.Topology)}
     * @param primaryTopologyAddress
     */
    public void setPrimaryTopologyAddress(String primaryTopologyAddress) {
        PrimaryTopologyAddress = primaryTopologyAddress;
    }

    public String getPrimaryTopologyAddress() {
        return PrimaryTopologyAddress;
    }


    public String getNoMatchTopologyAddress() {
        return NoMatchTopologyAddress;
//    return "merlin_default";
    }

    public void setNoMatchTopologyAddress(String noMatchTopologyAddress) {

        NoMatchTopologyAddress = noMatchTopologyAddress;

    }

    public String getNoMatchForwardingQueueFQQN(){

        String forwardingQueueFQQN = this.NoMatchTopologyAddress + "::" + this.NoMatchTopologyAddress_forwarding_queue;
        return forwardingQueueFQQN;
    }


    public void setNoMatchTopologyAddress_forwarding_queue(String noMatchTopologyAddress_forwarding_queue) {

        NoMatchTopologyAddress_forwarding_queue = noMatchTopologyAddress_forwarding_queue;
//        NoMatchTopologyAddress_forwarding_queue = "merlin_default_to_be_forwarded_to_parent";
    }

    public String getPrimary_catch_all_divert_name() {
        return primary_catch_all_divert_name;
    }

    public String getPrimary_catch_all_divert_filter_expression() {
        return primary_catch_all_divert_filter_expression;
    }


    public String getMetricsTopologyAddress() {
        return metricsAddress;
    }

    /****************************************************************************************************************
     *                              Operator Topology Management
     ****************************************************************************************************************/


    public void addOperatorAndAddress(Operator operator, String operatorAddress){


        Pair <Operator,String> operatorTobeAdded =  new MutablePair<>(operator, operatorAddress);
        operatorAndAddressList.add(operatorTobeAdded);

    }


    public boolean operatorAddressExists(String operatorAddressToFind){

        boolean operatorExists = false;
        for (Pair <Operator,String> p : this.operatorAndAddressList){

            if (operatorAddressToFind.compareTo(p.getValue()) ==0){
                operatorExists = true;
            }

        }
        return operatorExists;
    }

    public ArrayList<Pair<Operator, String>> getOperatorAndAddressList() {
        return operatorAndAddressList;
    }



    public String getOperatorAddress(Operator operatorToFind){

        String operatorAddress = null;

        for (Pair <Operator,String> p : this.operatorAndAddressList){
            if (p.getKey() == operatorToFind){
                operatorAddress = p.getValue();
                break;
            }
        }

        if (operatorAddress==null){
            throw new TopologyException("Could not find an address for operator: "+ operatorToFind.getOperatorID().getOperatorID_as_String()  + " on topology: " +  topologyID, new Throwable());
        }else{
            return operatorAddress;
        }

    }

    /****************************************************************************************************************
     *                              Attribute Address Management
     ****************************************************************************************************************/


                        /**This is managed by the respective {@link TopologyOperatorAddressManager}*/



    /****************************************************************************************************************
     *
     *                              Operator Installation
     *
     ****************************************************************************************************************/


    public void installOperator(Operator operator){
        this.installedOperators.add(operator);
    }

    /**
     * This method removes the operator from the operator list that is managed by this class
     * @param operator
     */
    public void uninstallOperator(Operator operator){

        this.installedOperators.remove(operator);

    }

    public ArrayList<Operator> getInstalledOperators() {
        return installedOperators;
    }

    /**
     * The function searches to see if at least one operator of still type still exists.
     * This function is mainly used to determine if a divert can be removed for this tuple type.
     * @param operatorType
     * @return
     */
    public boolean isOperatorTypeInstalled(String operatorType){

        boolean operatorTypeExists = false;
        for (Operator operator : this.installedOperators){

            if (operator.getInputType().equals(operatorType)){
                operatorTypeExists = true;
            }

        }
        return operatorTypeExists;
    }



    public Operator findInstalledOperatorByID(String operatorID){

        Operator foundOperator = null;
        for(Operator o: this.installedOperators){
            if(o.getOperatorID().getOperatorID_as_String().equalsIgnoreCase(operatorID)){
                foundOperator = o;
            }

        }

        return foundOperator;
    }


    /**
     * TODO: This function returns the first found operator of a given type which may
     *  not be the specific one that needs to be removed. Need to add functionality to
     *  identify specific operator replicas.
     *
     * @param operatorInputType
     * @return
     */
    public Operator findInstalledOperatorByType(String operatorInputType){

        Operator foundOperator = null;
        for(Operator o: this.installedOperators){
            if(o.getInputType().equalsIgnoreCase(operatorInputType)){
                foundOperator = o;
            }

        }

        return foundOperator;
    }



}
