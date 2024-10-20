package com.edgestream.worker.metrics.metricscollector2.io;

import com.edgestream.worker.metrics.common.InputEventMetric;
import com.edgestream.worker.metrics.common.OutputEventMetric;
import com.edgestream.worker.metrics.metricscollector2.ContainerMetricsDB;
import com.edgestream.worker.metrics.metricscollector2.derby.ContainerMetricsDerbyDB;
import org.apache.commons.lang3.SerializationUtils;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.concurrent.ConcurrentLinkedQueue;


/****************************************************************************************************
 * This class runs inside the EdgeStreamApplicationContext and its purpose is to collect
 * INPUT and OUTPUT EVENT metrics from the operator thread and insert them into to the database.
 *
 * (3) threads are crated here.
 *      1. The ZMQ socket to receive INPUT events
 *      2. The ZMQ socket to receive OUTPUT events
 *      3. The QueueChecker thread to check the queues and update the database.
 *
 * We use ConcurrentLinkedQueue to share data between the sockets threads and QueueChecker thread in a non blocking way.
 *****************************************************************************************************/

public class ZMQContainerMetricsService {
    /**INPUT**/
    private final ConcurrentLinkedQueue INPUT_concurrentLinkedQueue = new ConcurrentLinkedQueue();
    private final ZMQOperatorINPUTMetricsReceiver zmqOperatorINPUTMetricsReceiver;
    /**OUTPUT**/
    private final ConcurrentLinkedQueue OUTPUT_concurrentLinkedQueue = new ConcurrentLinkedQueue();
    private final ZMQOperatorOUTPUTMetricsReceiver zmqOperatorOUTPUTMetricsReceiver;

    /**GENERIC WRITER*/
    private final QueueChecker queueChecker;

    public ZMQContainerMetricsService(ContainerMetricsDerbyDB containerMetricsDerbyDB, String metricsInputEventPortNumber, String metricsOutputEventPortNumber) {
        zmqOperatorINPUTMetricsReceiver = new ZMQOperatorINPUTMetricsReceiver(metricsInputEventPortNumber,INPUT_concurrentLinkedQueue,"Operator [INPUT] Metrics Thread");
        zmqOperatorOUTPUTMetricsReceiver = new ZMQOperatorOUTPUTMetricsReceiver(metricsOutputEventPortNumber,OUTPUT_concurrentLinkedQueue,"Operator [OUTPUT] Metrics Thread");

        zmqOperatorINPUTMetricsReceiver.start();
        zmqOperatorOUTPUTMetricsReceiver.start();

        this.queueChecker = new QueueChecker(INPUT_concurrentLinkedQueue, OUTPUT_concurrentLinkedQueue, containerMetricsDerbyDB,"QueueChecker Thread");
        this.queueChecker.start();

    }

    private class QueueChecker implements Runnable{

        private Thread t;
        private final String tName;
        private final ConcurrentLinkedQueue INPUT_concurrentLinkedQueue;
        private final ConcurrentLinkedQueue OUTPUT_concurrentLinkedQueue;
        private final ContainerMetricsDB containerMetricsDB;

        public QueueChecker(ConcurrentLinkedQueue INPUT_concurrentLinkedQueue, ConcurrentLinkedQueue OUTPUT_concurrentLinkedQueue, ContainerMetricsDB containerMetricsDB, String tName) {

            this.tName = tName;
            this.INPUT_concurrentLinkedQueue = INPUT_concurrentLinkedQueue;
            this.OUTPUT_concurrentLinkedQueue = OUTPUT_concurrentLinkedQueue;
            this.containerMetricsDB = containerMetricsDB;
        }

        @Override
        public void run() {
            while(true) {
                checkQueueAndUpdateDB();
                try {
                    Thread.sleep(1000 *5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public void checkQueueAndUpdateDB(){

            while(!INPUT_concurrentLinkedQueue.isEmpty()){
                InputEventMetric inputEventMetric = SerializationUtils.deserialize((byte[]) INPUT_concurrentLinkedQueue.poll()); //the call to poll should return and remove the metric from the queue
                containerMetricsDB.insertInputEvent(inputEventMetric);
            }
            while(!OUTPUT_concurrentLinkedQueue.isEmpty()){
                OutputEventMetric outputEventMetric = SerializationUtils.deserialize((byte[]) OUTPUT_concurrentLinkedQueue.poll()); //the call to poll should return and remove the metric from the queue
                containerMetricsDB.insertOutputEvent(outputEventMetric);
            }
        }

        public void start() {
            System.out.println("[QueueChecker]: Starting " +  this.tName );
            if (t == null) {
                t = new Thread (this, tName);
                t.start();
            }
        }
    }


    /******************************************************************************
     *
     *
     *
     *              INPUT AND OUTPUT EVENT receiver threads
     *
     *
     **********************************************************************************/




    private class ZMQOperatorINPUTMetricsReceiver implements Runnable{

        private final ZContext context;
        private final ZMQ.Socket subSocket;
        private Thread t;
        private final String tName;
        private int counter =0;
        private final ConcurrentLinkedQueue INPUT_concurrentLinkedQueue;

        public ZMQOperatorINPUTMetricsReceiver(String metricsInputEventPortNumber, ConcurrentLinkedQueue  INPUT_concurrentLinkedQueue, String tName) {
            this.tName = tName;
            this.context = new ZContext();
            this.INPUT_concurrentLinkedQueue = INPUT_concurrentLinkedQueue;

            subSocket = context.createSocket(SocketType.SUB);
            subSocket.subscribe("".getBytes());
            subSocket.setReceiveTimeOut(5000);
            subSocket.connect("tcp://localhost:"+ metricsInputEventPortNumber); //TODO: test this with IPC sockets

        }


        private void parseInputMetric(byte[] incomingPayloadByteArray){
             this.INPUT_concurrentLinkedQueue.add(incomingPayloadByteArray);
        }

        @Override
        public void run() {
            System.out.println("[ZMQOperatorINPUTMetricsReceiver]: Receiver thread started");
            while(true) {
                byte[] incomingPayloadByteArray = subSocket.recv(0);
                if(incomingPayloadByteArray !=null) {
                    //System.out.println("[ZMQOperatorINPUTMetricsReceiver]: I received a warm up tuple from my consumer!"); //for debug
                    parseInputMetric(incomingPayloadByteArray);
                    counter++;
                }else{
                    System.out.println("[ZMQOperatorINPUTMetricsReceiver]: I did not receive a metric from my consumer in the last 5 seconds");
                    System.out.println("[ZMQOperatorINPUTMetricsReceiver]: Metrics received since the start: [" + counter + "]");
                }
            }
        }

        public void start() {
            System.out.println("[ZMQOperatorINPUTMetricsReceiver]: Starting " +  this.tName );
            if (t == null) {
                t = new Thread (this, tName);
                t.start();
            }
        }
    }




    private class ZMQOperatorOUTPUTMetricsReceiver implements Runnable{

        private final ZContext context;
        private final ZMQ.Socket subSocket;
        private Thread t;
        private final String tName;
        private int counter =0;
        private final ConcurrentLinkedQueue INPUT_concurrentLinkedQueue;

        public ZMQOperatorOUTPUTMetricsReceiver(String metricsInputEventPortNumber, ConcurrentLinkedQueue  INPUT_concurrentLinkedQueue, String tName) {
            this.tName = tName;
            this.context = new ZContext();
            this.INPUT_concurrentLinkedQueue = INPUT_concurrentLinkedQueue;

            subSocket = context.createSocket(SocketType.SUB);
            subSocket.subscribe("".getBytes());
            subSocket.setReceiveTimeOut(5000);
            subSocket.connect("tcp://localhost:"+ metricsInputEventPortNumber); //TODO: test this with IPC sockets

        }


        private void parseOutputMetric(byte[] incomingPayloadByteArray){
            this.INPUT_concurrentLinkedQueue.add(incomingPayloadByteArray);
        }

        @Override
        public void run() {
            System.out.println("[ZMQOperatorOUTPUTMetricsReceiver]: Receiver thread started");
            while(true) {
                byte[] incomingPayloadByteArray = subSocket.recv(0);
                if(incomingPayloadByteArray !=null) {
                    //System.out.println("[ZMQOperatorOUTPUTMetricsReceiver]: I received a warm up tuple from my consumer!"); //for debug
                    parseOutputMetric(incomingPayloadByteArray);
                    counter++;
                }else{
                    System.out.println("[ZMQOperatorOUTPUTMetricsReceiver]: I did not receive a metric from my consumer in the last 5 seconds");
                    System.out.println("[ZMQOperatorOUTPUTMetricsReceiver]: Metrics received since the start: [" + counter + "]");
                }
            }
        }

        public void start() {
            System.out.println("[ZMQOperatorINPUTMetricsReceiver]: Starting " +  this.tName );
            if (t == null) {
                t = new Thread (this, tName);
                t.start();
            }
        }
    }


}
