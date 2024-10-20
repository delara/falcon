package com.edgestream.worker.testing;

import com.edgestream.worker.runtime.container.EdgeStreamContainer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ListContainersCmd;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Statistics;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.InvocationBuilder;
//import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DockerStatsTest {


   public static void main (String[] args) throws InterruptedException {


       DockerClient dockerClient = DockerClientBuilder.getInstance("tcp://10.70.20.196:2375").build();

       //ListContainersCmd listContainersCmd = dockerClient.listContainersCmd().withStatusFilter(Collections.singleton("running"));
       ListContainersCmd listContainersCmd = dockerClient.listContainersCmd().withIdFilter(Collections.singleton("35c0442ce782"));


        //and set the generic filter regarding name
       //listContainersCmd.getFilters().put("name", Arrays.asList("funny_raman"));
        //finally, run the command
       List<Container> exec = listContainersCmd.exec();


       while(true) {
           for (Container c : exec) {

               Statistics stats = getNextStatistics(dockerClient, c.getId());
               System.out.println(c.getId());
               System.out.println(c.getNames()[0]);
               System.out.println("System cpu: " + stats.getCpuStats().getSystemCpuUsage());
               System.out.println(calculateCPUUsage(stats));
           }
           Thread.sleep(1000);

       }



   }


    private static double calculateCPUUsage(Statistics stats){


        //cpuPercent = 0.0
        //calculate the change for the cpu usage of the container in between readings
        //cpu_delta = float(jsondata['cpu_stats']['cpu_usage']['total_usage']) - float(jsondata['precpu_stats']['cpu_usage']['total_usage'])
        // # calculate the change for the entire system between readings

        //        systemDelta = float(jsondata['cpu_stats']['system_cpu_usage']) - float(jsondata['precpu_stats']['system_cpu_usage'])

        //if systemDelta > 0.0 and cpu_delta > 0.0:
        //cpuPercent = (cpu_delta / systemDelta) * 4 * 100.0

        double cpuPercent = 0.0;

        /**CPU delta*/
        double totalUsage =  stats.getCpuStats().getCpuUsage().getTotalUsage();
        double prev_totalUsage = stats.getPreCpuStats().getCpuUsage().getTotalUsage();

        /**System delta*/

        double systemTotalUsage = stats.getCpuStats().getSystemCpuUsage();

        double prev_systemTotalUsage = 0.0;
        if (stats.getPreCpuStats().getSystemCpuUsage() !=null){
            prev_systemTotalUsage = stats.getPreCpuStats().getSystemCpuUsage();
        }else{
            System.out.println("no pre system cpu yet");
        }






        try {
            double cpuDelta = totalUsage - prev_totalUsage;
            double sysDelta =  systemTotalUsage - prev_systemTotalUsage;

            if (sysDelta > 0.0 && cpuDelta > 0.0) {
                cpuPercent = (cpuDelta / sysDelta) * 100;
            }
        }catch (Exception e){
            if (e.getClass() == NullPointerException.class) {
                System.out.println("No pre-cpu stats yet.. will call again in 1 second");

            }
        }

        return cpuPercent;

    }

    static public Statistics getNextStatistics(DockerClient dockerClient, String id) {
        InvocationBuilder.AsyncResultCallback<Statistics> callback = new InvocationBuilder.AsyncResultCallback<>();
        dockerClient.statsCmd(id).exec(callback);
        Statistics stats = null;
        try {
            stats = callback.awaitResult();
            callback.close();
        } catch (RuntimeException | IOException e) {
            // you may want to throw an exception here
        }
        return stats; // this may be null or invalid if the container has terminated
    }


}
