package com.edgestream.worker.runtime.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.ListImagesCmd;
import com.github.dockerjava.api.model.Image;
import com.github.dockerjava.core.DockerClientBuilder;

import java.util.List;


public class DockerClientTestConnectivity {

    public static void main(String[] args) {

        String dockerHostIP = "10.70.2.12";
        String dockerRemoteAPIPort = "2375";
        DockerClient dockerClient = DockerClientBuilder.getInstance("tcp://" + dockerHostIP +":" + dockerRemoteAPIPort).build();

        ListImagesCmd listImagesCmd = dockerClient.listImagesCmd();
        List<Image> images = listImagesCmd.exec();

        System.out.println(images);
    }
}
