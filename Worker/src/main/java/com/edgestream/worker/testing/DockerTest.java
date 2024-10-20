package com.edgestream.worker.testing;

import com.edgestream.worker.runtime.docker.GetContainerLog;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.BuildImageResultCallback;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Statistics;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.InvocationBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DockerTest {


   public static void main (String[] args) throws InterruptedException {





       //DockerClient dockerClient = DockerClientBuilder.getInstance("tcp://192.168.86.35:2375").build();
       DockerClient dockerClient = DockerClientBuilder.getInstance("tcp://10.70.20.196:2375").build();

       //String JarfileAndPath = "C:\\Users\\brianr\\IdeaProjects\\edgestream\\benchmarks\\sentiment-analysis\\target\\sentiment-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar";

       //String localJar = "sentiment-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar";
       String dockerFileAndPath= "C:\\Jar\\Dockerfile";

       String imageId = dockerClient.buildImageCmd()
               .withDockerfile(new File(dockerFileAndPath))
               .withPull(true)
               //.withBuildArg("jarFile",JarfileAndPath)
               .withNoCache(true)
               .exec(new BuildImageResultCallback())
               .awaitImageId();


       String repository = "brianr/merlinoperator";
       String tag = "latest";

       dockerClient.tagImageCmd(imageId, repository, tag).exec();


       //////////////////////////////////////////////////////////////////////////////////////////////

       String jarFileWithPath = "app.jar";
       String jarParams ="PCEB";
       String operatorInputBrokerIP = "tcp://10.70.20.196:61616";
       String operatorInputQueue ="merlin_default::merlin_default_to_be_forwarded_to_parent";
       String operatorOutputBrokerIP = "tcp://10.70.20.196:61616";
       String operatorOutputAddress ="merlin_default";
       String topologyID = "TP001";
       String node_ID = "W004" ;
       String metrics_broker_IP_address = "tcp://10.70.20.196:61616";
       String metrics_FQQN ="primary_merlin_metrics_address";
       String operatorID = "OP_001";

       String containerName = node_ID + "_" + topologyID + "_" + operatorID;




       List<String> env = new ArrayList<>();
       env.add("jarFile=" + jarFileWithPath);
       env.add("jarName=" + "app.jar");
       env.add("jarParams=" + jarParams);
       env.add("operatorInputBrokerIP=" +operatorInputBrokerIP);
       env.add("operatorInputQueue=" +operatorInputQueue);
       env.add("operatorOutputBrokerIP=" + operatorOutputBrokerIP);
       env.add("operatorOutputAddress=" +operatorOutputAddress);
       env.add("topologyID=" +topologyID);
       env.add("node_ID=" +node_ID);
       env.add("metrics_broker_IP_address=" +metrics_broker_IP_address);
       env.add("metrics_FQQN=" +metrics_FQQN);
       env.add("operatorID=" +operatorID);


       HostConfig hostConfig = new HostConfig();
       hostConfig.withMemory(1000000000l); //2gb memory limit

       CreateContainerCmd containerCMD = dockerClient.createContainerCmd("busybox");
       containerCMD.withHostConfig(hostConfig);
       containerCMD.withName("funny_raman");
       //containerCMD.withEnv(env);


       CreateContainerResponse container = containerCMD.exec();








       GetContainerLog getContainerLog = new GetContainerLog(dockerClient,container.getId());


       dockerClient.startContainerCmd(container.getId()).exec();


       System.out.println(getContainerLog.getDockerLogs().toString());

       Thread.sleep(1000 * 10);


       Statistics stats = getNextStatistics(dockerClient, container.getId());

       System.out.println("System cpu" + stats.getCpuStats().getSystemCpuUsage());


       dockerClient.stopContainerCmd(container.getId()).exec();
       dockerClient.removeContainerCmd(container.getId()).exec();

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
