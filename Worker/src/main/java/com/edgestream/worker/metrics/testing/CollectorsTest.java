package com.edgestream.worker.metrics.testing;

import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.metrics.metricscollector2.operator.OperatorCollector;
import com.edgestream.worker.metrics.metricscollector2.stream.StreamCollector;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class CollectorsTest {
//    public static void main(String[] args) throws InterruptedException {
//        OperatorCollector operatorCollector = new OperatorCollector(null, null, "T", "N", "O", "B1", "B2", "300");
//        ZonedDateTime time = ZonedDateTime.now().minusSeconds(30);
//
//        for (int i = 0; i < 38; i++) {
//            operatorCollector.addInput(100L);
//            operatorCollector.addOutput(20L, time.toString(), 50L);
//            TimeUnit.MILLISECONDS.sleep(200);
//            time = time.plusNanos(200000000);
//        }
//
//        operatorCollector.publish(false);
//    }

//    public static void main(String[] args) throws Exception {
//        StreamCollector streamCollector = new StreamCollector(null, null, "T", "N", "O", "B1", "B2", "300");
//        ZonedDateTime time = ZonedDateTime.now().minusSeconds(30);
//        ArrayList<String> list = new ArrayList<>();
//        list.add("A");
//        list.add("B");
//        list.add("C");
//
//        int count = 0;
//        for (int i = 0; i < 38; i++) {
//            streamCollector.add(list.get(count), time.toString(), 20L);
//            TimeUnit.MILLISECONDS.sleep(200);
//            time = time.plusNanos(200000000);
//            count++;
//            if (count > 2){
//                count = 0;
//            }
//        }
//
//        streamCollector.publish(false);
//    }

    public static void main(String[] args) throws Exception {
        MetricsCollector3 metrics = new MetricsCollector3(null, null, "T", "N", "O", "B1", "B2", "300");

        OperatorCollector operatorCollector = metrics.getOperatorCollector();
        ZonedDateTime time = ZonedDateTime.now().minusSeconds(30);

        for (int i = 0; i < 300; i++) {
            operatorCollector.addInput(100L);
            operatorCollector.addOutput(20L, time.toString(), 50L);
            TimeUnit.MILLISECONDS.sleep(200);
            time = time.plusNanos(200000000);
        }

        metrics.close();


        System.out.println("\n\n\n Success!!");

    }
}
