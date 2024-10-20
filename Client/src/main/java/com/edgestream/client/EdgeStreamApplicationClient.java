package com.edgestream.client;

import com.edgestream.worker.metrics.metricscollector2.MetricsCollector2;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.runtime.reconfiguration.state.AtomicKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;

public class EdgeStreamApplicationClient {



    private final String inputType;
    private final String operatorInputBrokerIP;
    private final String operatorInputQueue;
    private final String operatorOutputBrokerIP;
    private final String operatorOutputAddress;
    private final String topologyID;
    private final String taskManagerID;
    private final String metrics_broker_IP_address;
    private final String metrics_FQQN;
    private final String operator_ID_asString;
    private final String predecessorInputMethod;
    private final String predecessorIP;
    private final String predecessorPort;
    private final String successorIP;
    private final String outputTypeAndPortList;
    private final String stateObject;
    private final String reconfigurationPlanID;
    private final String TMToOPManagementFQQN;
    private final String OPtoOPManagementFQQN;
    private final String localOperatorMetricsFQQN;
    private final String warmupFlag;
    private final String warmupContainerIP;
    private final String warmupContainerOperatorID;
    private final String notifyPredecessorWhenReady;
    private final String waitForSuccessorReadyMessage;


    private final EdgeStreamApplicationConfig edgeStreamApplicationConfig;
    private final MetricsCollector3 metricsCollector;


    HashMap<String, String> typeToPortMap;
    ArrayList<AtomicKey> routingKeys = new ArrayList<>();


    int producerBatchSize              = 1;      //IGNORED by the metrics collector
    int consumerBatchSize              = 1;      //used by the metrics collector
    int producerBufferSize             = 3000000;  //used by the metrics collector
    int consumerBufferSize             = 3000000;  //used by the metrics collector


    public EdgeStreamApplicationClient(String[] args) {

        System.out.println("Received Params: " + Arrays.toString(args)) ;

        this.inputType                    = args[0];
        this.operatorInputBrokerIP        = args[1];
        this.operatorInputQueue           = args[2];
        this.operatorOutputBrokerIP       = args[3];
        this.operatorOutputAddress        = args[4];

        this.topologyID                   = args[5];
        this.taskManagerID                = args[6];
        this.metrics_broker_IP_address    = args[7];
        this.metrics_FQQN                 = args[8];
        this.operator_ID_asString         = args[9];

        this.predecessorInputMethod       = args[10]; // valid values: activemq or zeromq
        this.predecessorIP                = args[11]; //predecessor ip
        this.predecessorPort              = args[12]; //predecessor output port, ONLY used for consuming data.
        this.successorIP                  = args[13]; //This is used when a producer is started after the consumer, ie a new source had been added to a running application
        this.outputTypeAndPortList        = args[14]; //this arg

        this.stateObject                  = args[15];
        this.reconfigurationPlanID        = args[16];
        this.TMToOPManagementFQQN         = args[17];
        this.OPtoOPManagementFQQN         = args[18];
        this.localOperatorMetricsFQQN     = args[19];
        this.warmupFlag                   = args[20];
        this.warmupContainerIP            = args[21];
        this.warmupContainerOperatorID    = args[22];
        this.notifyPredecessorWhenReady   = args[23];
        this.waitForSuccessorReadyMessage = args[24];

        parseOutputTypeAndPort(this.outputTypeAndPortList);
        parseStateObject(stateObject);

        this.metricsCollector = new MetricsCollector3(metrics_broker_IP_address, metrics_FQQN, topologyID, taskManagerID
                , operator_ID_asString,String.valueOf(consumerBufferSize),String.valueOf(producerBufferSize),String.valueOf(consumerBatchSize));


        this.edgeStreamApplicationConfig = new EdgeStreamApplicationConfig(operatorInputBrokerIP,operatorInputQueue,operatorOutputBrokerIP
                ,operatorOutputAddress,topologyID,taskManagerID,metrics_broker_IP_address,metrics_FQQN,producerBatchSize,consumerBatchSize,producerBufferSize
                ,consumerBufferSize, predecessorInputMethod, predecessorIP, predecessorPort, successorIP, typeToPortMap, reconfigurationPlanID, TMToOPManagementFQQN, OPtoOPManagementFQQN, localOperatorMetricsFQQN
                ,warmupFlag,warmupContainerIP,warmupContainerOperatorID,notifyPredecessorWhenReady,waitForSuccessorReadyMessage);


        System.out.println("Warmup details ["+ warmupFlag +"][" + warmupContainerIP + "]" );
    }





    public EdgeStreamApplicationConfig getEdgeStreamApplicationConfig() {
        return edgeStreamApplicationConfig;
    }

    public MetricsCollector3 getMetricsCollector() {
        return metricsCollector;
    }




    public String getInputType() {
        return inputType;
    }



    public String getOperator_ID_asString() {
        return operator_ID_asString;
    }


    public HashMap parseOutputTypeAndPort(String typeToPortMapping) {

        StringTokenizer st = new StringTokenizer(typeToPortMapping, "|");

        HashMap<String, String> typeToPortMap = new HashMap<>();
        while (st.hasMoreTokens()) {
            String[] elements = st.nextToken().split(",");
            typeToPortMap.put(elements[0], elements[1]);
        }

        //System.out.println(typeToPortMap.get("F"));
        //System.out.println(typeToPortMap.get("S"));
        this.typeToPortMap = typeToPortMap;
        return this.typeToPortMap;

    }

    public void parseStateObject(String stateObject) {
        StringTokenizer st = new StringTokenizer(stateObject, "|");

        while (st.hasMoreTokens()) {
            String[] elements = st.nextToken().split(",");
            this.routingKeys.add(new AtomicKey(elements[0],elements[1]));
        }
    }

    public ArrayList<AtomicKey> getRoutingKeys() {
        return this.routingKeys;
    }

}
