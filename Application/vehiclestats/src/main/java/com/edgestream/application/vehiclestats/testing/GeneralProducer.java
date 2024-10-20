package com.edgestream.application.vehiclestats.testing;

public class GeneralProducer {
    public static void main(String[] args) throws Exception {
        String outputType = args[0];
        String target_IP_recipient = args[1];
        String fqqn = args[2];
        String topologyID = args[3];
        String tupleOrigin = args[4];
        double productionRate = Double.parseDouble(args[5]); //Images per second
        String producerId = args[6];

//        new ArtemisImgProducer("Producer : " + producer_id, producer_id, outputType, productionRate, target_IP_recipient, dataSetPath, fqqn, topologyID, tupleOrigin,total_to_send).start();

        new VehicleStatsProducer( outputType, productionRate, target_IP_recipient, fqqn, topologyID, tupleOrigin, producerId).start();

//        0 R tcp://0.0.0.0:61616 op1::o1 /home/aveith/projects/edgestream/Application/objectdetection/src/main/resources/dt/ TEST null 1
    }
}
