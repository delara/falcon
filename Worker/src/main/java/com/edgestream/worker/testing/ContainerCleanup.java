package com.edgestream.worker.testing;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DockerClientBuilder;

import java.util.ArrayList;
import java.util.List;

public class ContainerCleanup {


    DockerClient dockerClient;

    public ContainerCleanup(){


    }

    public static void main (String[] args) throws InterruptedException {


        //docker run -it -e "jarFile=sentiment-analysis-1.0-SNAPSHOT-jar-with-dependencies.jar" -e "jarParams=PCEB" -e "operatorInputBrokerIP=tcp://10.70.20.47:61616" brianr/merlinoperator:latest

        ContainerCleanup containerCleanup = new ContainerCleanup();

        containerCleanup.dockerClient = DockerClientBuilder.getInstance("tcp://10.70.20.196:2375").build();

        String containerName ="test_container_01";
        containerCleanup.ifExistsRemoveContainer(containerName);



        CreateContainerCmd containerCMD = containerCleanup.dockerClient.createContainerCmd("brianr/merlinoperator:latest");
        containerCMD.withName(containerName);

        CreateContainerResponse container = containerCMD.exec();
        containerCleanup.dockerClient.startContainerCmd(container.getId()).exec();


    }


    private void ifExistsRemoveContainer(String containerName){

        System.out.println("Checking to see if any active containers are running");
        List<String> containerList = new ArrayList<>();
        containerList.add(containerName);

        List<Container> containers = dockerClient.listContainersCmd()
                .withNameFilter(containerList)
                .withShowSize(true)
                .withShowAll(true)
                //.withStatusFilter(Collections.singleton("created"))
                .exec();


        /*
        for(Container  c: containers){

            String[] container_name= c.getNames();
            System.out.println("container:" + container_name[0]);
        }


        */

        if (containers.size() > 0) {
            for (Container c : containers) {

                String[] container_name = c.getNames();
                System.out.println("Found matching container: " + container_name[0]);

                if(!c.getState().equalsIgnoreCase("exited")) {
                    System.out.println("Stopping container" + container_name[0] + "....");
                    dockerClient.stopContainerCmd(c.getId()).exec();
                }

                System.out.println("Removing container" + container_name[0] + "....");
                dockerClient.removeContainerCmd(c.getId()).exec();

                //recursively check again if the container has now been removed
                ifExistsRemoveContainer(containerName);

            }
        }else{

            System.out.println("Container: " + containerName + " does not yet exist");

        }


    }


    }
