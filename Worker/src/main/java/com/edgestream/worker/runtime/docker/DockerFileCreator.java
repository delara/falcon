package com.edgestream.worker.runtime.docker;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class DockerFileCreator {

    DockerFile dockerFile;


    public DockerFileCreator() {

    }


    public DockerFile createDockerFile(DockerFileType type, String destinationPath) throws IOException {


        if (type.equals(DockerFileType.EDGENT)){

            dockerFile = createEdgentDockerFile(destinationPath);
            return dockerFile;

        }



        if (type.equals(DockerFileType.FACEDETECTION)){

            dockerFile = createFaceDetectionDockerFile(destinationPath);
            return dockerFile;

        }
        if (type.equals(DockerFileType.EDGESTREAM_STATEFUL)){

            dockerFile = createEdgeStreamStatefulDockerFile(destinationPath);
            return dockerFile;

        }

        if (type.equals(DockerFileType.JAR_SPOUT)){

            dockerFile = createJarSpoutDockerFile(destinationPath);
            return dockerFile;

        }

        return dockerFile;

    }


    private DockerFile createEdgentDockerFile(String destinationPath) throws IOException {

        String edgentDockerFile = "#EdgeStream Edgent Operator\n" +
                "\n" +
                "##########################################################\n" +
                "## Build Image                                           #\n" +
                "##########################################################\n" +
                "\n" +
                "\n" +
                "#docker build --no-cache -t brianr/merlinoperator:latest .\n" +
                "\n" +
                "#git fetch origin ; git checkout master ;git merge origin/master\n" +
                "\n" +
                "#docker run -i brianr/merlinworker W003 192.168.86.35  W001 10.10.10.9\n" +
                "\n" +
                "#docker run -i brianr/merlinworker W001 10.70.20.196 W001 10.70.2.109\n" +
                "#docker run -i brianr/merlinworker W002 10.70.20.47 W001 10.70.2.109\n" +
                "#docker run -i brianr/merlinworker W003 10.70.20.48 W001 10.70.2.109\n" +
                "\n" +
                //"FROM openjdk:11-jdk as intermediate\n" +
                "FROM openjdk:11-jdk\n" +
                "\n" +
                "# install git\n" +
                //"RUN apt-get update\n" +
                //"RUN apt-get install -y git\n" +
                "ARG JARFILE=\"nofile.jar\"\n" +
                "RUN echo ${JARFILE}\n"+
                "RUN mkdir -p app\n"+
                "WORKDIR /app\n"+
                "COPY ${JARFILE} .\n"+
                "#copy the jar file from local filesystem to container root dir\n" +
                "#WORKDIR /\n" +
                "#COPY $jarFile /app\n" +
                "#RUN [\"ls\"]\n" +
                "#WORKDIR /app\n" +
                "\n" +
                "CMD java -jar /app/$jarName $operatorType $operatorInputBrokerIP $operatorInputQueue $operatorOutputBrokerIP $operatorOutputAddress $topologyID $taskManagerID $metrics_broker_IP_address $metrics_FQQN $operatorID $predecessorInputMethod $predecessorIP $predecessorPort $successorIP $outputTypeToPortMap\n";

        File file = new File(destinationPath+"/Dockerfile");


        FileWriter writer = new FileWriter(file);
        writer.write(edgentDockerFile);
        writer.close();


        DockerFile dockerFile = new DockerFile(edgentDockerFile,DockerFileType.EDGENT,destinationPath);
        return dockerFile;


    }


    private DockerFile createEdgeStreamStatefulDockerFile(String destinationPath) throws IOException {

        String edgeStreamStatefulDockerFile = "#EDGESTREAM_STATEFUL Operator\n" +
                "\n" +
                "##########################################################\n" +
                "## Build Image                                           #\n" +
                "##########################################################\n" +
                "\n" +
                "\n" +
                "#docker build --no-cache -t brianr/merlinoperator:latest .\n" +
                "\n" +
                "#git fetch origin ; git checkout master ;git merge origin/master\n" +
                "\n" +
                "#docker run -i brianr/merlinworker W003 192.168.86.35  W001 10.10.10.9\n" +
                "\n" +
                "#docker run -i brianr/merlinworker W001 10.70.20.196 W001 10.70.2.109\n" +
                "#docker run -i brianr/merlinworker W002 10.70.20.47 W001 10.70.2.109\n" +
                "#docker run -i brianr/merlinworker W003 10.70.20.48 W001 10.70.2.109\n" +
                "\n" +
                //"FROM openjdk:11-jdk as intermediate\n" +
                "FROM openjdk:11-jdk\n" +
                "\n" +
                "# install git\n" +
                //"RUN apt-get update\n" +
                //"RUN apt-get install -y git\n" +
                "ARG JARFILE=\"nofile.jar\"\n" +
                "RUN echo ${JARFILE}\n"+
                "RUN mkdir -p app\n"+
                "WORKDIR /app\n"+
                "COPY ${JARFILE} .\n"+
                "#copy the jar file from local filesystem to container root dir\n" +
                "#WORKDIR /\n" +
                "#COPY $jarFile /app\n" +
                "#RUN [\"ls\"]\n" +
                "#WORKDIR /app\n" +
                "\n" +
                "CMD java -Xms500m -Xmx2000m -jar /app/$jarName $operatorType $operatorInputBrokerIP $operatorInputQueue $operatorOutputBrokerIP $operatorOutputAddress $topologyID $taskManagerID $metrics_broker_IP_address $metrics_FQQN $operatorID $predecessorInputMethod $predecessorIP $predecessorPort $successorIP $outputTypeToPortMap $stateObject $reconfigurationPlanID $TMToOPManagementFQQN $OPtoOPManagementFQQN $localOperatorMetricsFQQN $warmupFlag $warmupContainerIP $warmupContainerOperatorID $notifyPredecessorWhenReady $waitForSuccessorReadyMessage\n";

        File file = new File(destinationPath+"/Dockerfile");


        FileWriter writer = new FileWriter(file);
        writer.write(edgeStreamStatefulDockerFile);
        writer.close();


        DockerFile dockerFile = new DockerFile(edgeStreamStatefulDockerFile,DockerFileType.EDGESTREAM_STATEFUL,destinationPath);
        return dockerFile;


    }



    private DockerFile createFaceDetectionDockerFile(String destinationPath) throws IOException {
/*
        String edgentDockerFile = "#EdgeStream Edgent Operator\n" +
                "\n" +
                "##########################################################\n" +
                "## Build Image                                           #\n" +
                "##########################################################\n" +
                "\n" +
                "\n" +
                "#docker build --no-cache -t brianr/merlinoperator:latest .\n" +
                "\n" +
                "#git fetch origin ; git checkout master ;git merge origin/master\n" +
                "\n" +
                "#docker run -i brianr/merlinworker W003 192.168.86.35  W001 10.10.10.9\n" +
                "\n" +
                "#docker run -i brianr/merlinworker W001 10.70.20.196 W001 10.70.2.109\n" +
                "#docker run -i brianr/merlinworker W002 10.70.20.47 W001 10.70.2.109\n" +
                "#docker run -i brianr/merlinworker W003 10.70.20.48 W001 10.70.2.109\n" +
                "\n" +
                //"FROM openjdk:11-jdk as intermediate\n" +
                "FROM openjdk:11-jdk\n" +
                "\n" +
                "# install git\n" +
                //"RUN apt-get update\n" +
                //"RUN apt-get install -y git\n" +
                "ARG JARFILE=\"nofile.jar\"\n" +
                "RUN apt-get update && apt-get install -y wget build-essential cmake libopencv-dev ffmpeg libjpeg-dev libpng-dev libtiff-dev libgtk2.0-dev libatlas-base-dev qt5-default libvtk6-dev\n"+
                "RUN echo ${JARFILE}\n"+
                "RUN mkdir -p app\n"+
                "WORKDIR /app\n"+
                "COPY ${JARFILE} .\n"+
                "#copy the jar file from local filesystem to container root dir\n" +
                "#WORKDIR /\n" +
                "#COPY $jarFile /app\n" +
                "#RUN [\"ls\"]\n" +
                "#WORKDIR /app\n" +
                "\n" +
                "CMD java -jar /app/$jarName $jarParams $operatorInputBrokerIP $operatorInputQueue $operatorOutputBrokerIP $operatorOutputAddress $topologyID $node_ID $metrics_broker_IP_address $metrics_FQQN $operatorID $predecessorInputMethod $predecessorIP $predecessorPort $successorIP $outputTypeToPortMap\n";

*/

        String edgentDockerFile = "#EdgeStream Edgent Operator\n" +
                "\n" +
                "##########################################################\n" +
                "## Build Image                                           #\n" +
                "##########################################################\n" +
                "\n" +
                "\n" +
                "#docker build --no-cache -t brianr/merlinoperator:latest .\n" +
                "\n" +
                "#git fetch origin ; git checkout master ;git merge origin/master\n" +
                "\n" +
                "#docker run -i brianr/merlinworker W003 192.168.86.35  W001 10.10.10.9\n" +
                "\n" +
                "#docker run -i brianr/merlinworker W001 10.70.20.196 W001 10.70.2.109\n" +
                "#docker run -i brianr/merlinworker W002 10.70.20.47 W001 10.70.2.109\n" +
                "#docker run -i brianr/merlinworker W003 10.70.20.48 W001 10.70.2.109\n" +
                "\n" +
                //"FROM openjdk:11-jdk as intermediate\n" +
                "FROM brianr82/edgestreamtopologytpfaced:latest\n" +
                "ARG JARFILE=\"nofile.jar\"\n" +
                "RUN echo ${JARFILE}\n"+
                "RUN mkdir -p app\n"+
                "WORKDIR /app\n"+
                "COPY ${JARFILE} .\n"+
                "#copy the jar file from local filesystem to container root dir\n" +
                "#WORKDIR /\n" +
                "#COPY $jarFile /app\n" +
                "#RUN [\"ls\"]\n" +
                "#WORKDIR /app\n" +
                "\n" +
                "CMD java -jar /app/$jarName $operatorType $operatorInputBrokerIP $operatorInputQueue $operatorOutputBrokerIP $operatorOutputAddress $topologyID $taskManagerID $metrics_broker_IP_address $metrics_FQQN $operatorID $predecessorInputMethod $predecessorIP $predecessorPort $successorIP $outputTypeToPortMap\n";


        File file = new File(destinationPath+"/Dockerfile");


        FileWriter writer = new FileWriter(file);
        writer.write(edgentDockerFile);
        writer.close();


        DockerFile dockerFile = new DockerFile(edgentDockerFile,DockerFileType.FACEDETECTION,destinationPath);
        return dockerFile;


    }







    private DockerFile createJarSpoutDockerFile(String destinationPath) throws IOException {

        String jarSpoutDockerFile = "#EDGESTREAM_Jar_Spout\n" +
                "\n" +
                "##########################################################\n" +
                "## Build Image                                           #\n" +
                "##########################################################\n" +
                "\n" +
                "FROM openjdk:11-jdk\n" +
                "\n" +
                "ARG JARFILE=\"nofile.jar\"\n" +
                "RUN echo ${JARFILE}\n"+
                "RUN mkdir -p app\n"+
                "RUN apt-get update\n"+
                "RUN apt install iputils-ping -y\n"+
                "RUN git clone https://github.com/brianr82/tweets.git\n" +
                "RUN ls\n"+
                "WORKDIR /app\n"+
                "COPY ${JARFILE} .\n"+
                "CMD java -jar /app/$jarName $numberProducers $runTime $containerHostName $containerPort $datasetPath\n";

        File file = new File(destinationPath+"/Dockerfile");


        FileWriter writer = new FileWriter(file);
        writer.write(jarSpoutDockerFile);
        writer.close();


        DockerFile dockerFile = new DockerFile(jarSpoutDockerFile,DockerFileType.JAR_SPOUT,destinationPath);
        return dockerFile;


    }


}
