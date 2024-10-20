package com.edgestream.worker.runtime.task.processor;

import com.edgestream.worker.runtime.container.EdgeStreamContainer;
import com.edgestream.worker.runtime.container.EdgeStreamContainerID;
import com.edgestream.worker.runtime.container.EdgeStreamOperatorContainer;
import com.edgestream.worker.runtime.docker.DockerAPI;
import com.edgestream.worker.runtime.docker.DockerFile;
import com.edgestream.worker.runtime.docker.DockerHost;
import com.edgestream.worker.runtime.docker.DockerHostManager;
import com.edgestream.worker.runtime.task.TaskDriver;
import com.edgestream.worker.runtime.task.TaskRunner;
import com.edgestream.worker.runtime.task.TaskRunnerExternal;
import com.edgestream.worker.runtime.task.model.TaskRequestSetupOperator;
import com.edgestream.worker.runtime.topology.Topology;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.BuildImageResultCallback;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.HostConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DockerConfig {

    private final DockerClient dockerClient;
    private final Topology topology;
    private String operatorImage;
    private final DockerFile dockerFile;
    private final TaskRequestSetupOperator taskRequestSetupOperator;
    private final DockerHost dockerHost;
    private final DockerHostManager dockerHostManager;
    private final String taskManagerID;
    private String dockerContainerName;
    private final PredecessorSuccessorConfig predecessorSuccessorConfig;



    protected DockerConfig(TaskRunnerExternal taskRunnerExternal
            , Topology topology
            , TaskRequestSetupOperator taskRequestSetupOperator
            , String taskManagerID
            , PredecessorSuccessorConfig predecessorSuccessorConfig
            , DockerHostManager dockerHostManager) {


        this.dockerHostManager = dockerHostManager; // The manager of all docker hosts in this datacenter
        this.dockerHost =  this.dockerHostManager.getDockerHost(taskRunnerExternal.getDockerHostID()); // The specific host that this new operator will be run on
        this.dockerClient =  this.dockerHost.getDockerClient(); //The client for the specific host that this operator will be run on
        this.topology = topology;
        this.dockerFile = taskRunnerExternal.getDockerFile();
        this.taskRequestSetupOperator = taskRequestSetupOperator;
        this.taskManagerID = taskManagerID;
        this.predecessorSuccessorConfig = predecessorSuccessorConfig;

        setOperatorImage();

    }



    private void setOperatorImage(){

        long buildStartTime = System.nanoTime();
        /**********************************************************************************************
         * Docker image build and setup
         **********************************************************************************************/
        if(!dockerHost.isDockerTopologyRepositoryAndTagSet()){
            System.out.println("[DockerConfig]: Topology image not set, will build");
            //build image once

            operatorImage = buildDockerTopologyImage(dockerFile.getDockerFilePath()+"Dockerfile");

            long timeToBuild = System.nanoTime() - buildStartTime;
            System.out.println(String.format("[DockerConfig]: docker build time %o",timeToBuild/1000000000l));
        }else{
            operatorImage = dockerHost.getTopologyDockerRepositoryAndTag();
        }
    }


    private String buildDockerTopologyImage(String dockerFilePath){

        System.out.println("[DockerConfig]: Operator code has finished downloading finished downloading from cloud cluster manager broker");
        System.out.println("[DockerConfig]: Building Docker image for Topology: " + topology.getTopologyID());


        String dockerFileAndPath= dockerFilePath;

        String imageId = dockerClient.buildImageCmd()
                .withDockerfile(new File(dockerFileAndPath))
                .withBuildArg("JARFILE",topology.getTopologyJarFileName())
                .withPull(true)
                .withNoCache(true) //TODO: change back to true after done testing
                .exec(new BuildImageResultCallback())
                .awaitImageId();


        String repository = "brianr/edgestreamtopology" + topology.getTopologyID().toLowerCase();
        String tag = "latest";

        dockerClient.tagImageCmd(imageId, repository, tag).exec();

        String repositoryAndTag = repository + ":" +tag;

        System.out.println("[DockerConfig]: Done building Docker image for EdgeStream topology: " + topology.getTopologyID());


        dockerHost.setTopologyDockerRepositoryAndTag(repositoryAndTag);

        return repositoryAndTag;

    }


    protected void instantiateOperatorContainer(
              String jarDirectory
            , String operatorInputBrokerIP
            , String operatorInputFQQN
            , String operatorOutputBrokerIP
            , String operatorOutputAddress
            , String metrics_broker_IP_address
            , String metrics_FQQN
            , String reconfigurationPlanID
            , TaskWarmerConfig taskWarmerConfig
            , RoutingKeyConfig routingKeyConfig
            ){

        /**********************************************************************************************************
         *                                  Pre container Setup BEGINS
         **********************************************************************************************************/

        /**Clean up any old containers first */

        System.out.println("[DockerConfig]: Attempting to create container for: " + taskRequestSetupOperator.getOperator().getOperatorID().getOperatorID_as_String());
        dockerContainerName = taskManagerID + "_" + topology.getTopologyID() + "_" + taskRequestSetupOperator.getOperator().getOperatorID().getOperatorID_as_String();
        this.dockerHostManager.findAndRemoveContainerFromDC(dockerContainerName);




        EdgeStreamContainerID edgeStreamContainerID = new EdgeStreamContainerID("EDGE_CON_" +topology.getTopologyID() + "_" + taskRequestSetupOperator.getOperator().getOperatorID().getOperatorID_as_String());
        EdgeStreamContainer edgeStreamContainer = new EdgeStreamOperatorContainer(dockerHost,edgeStreamContainerID,taskRequestSetupOperator.getOperatorType(),taskRequestSetupOperator.getOperator().getOperatorID());


        //1. Some operators output multiple tuples so we create output ports for all the tuple types this operator will output
        //   this is used as  lookup when setting up future successors and to generate the output port mapping instructions for this container
        for(String successor : predecessorSuccessorConfig.getSuccessors()) {

            edgeStreamContainer.addOutputTypeToPortMapping(successor); //the first mapping is always port 8000, the port number is automatically incremented every time this method is invoked
        }


        /**********************************************************************************************************
         *                                  Pre container Setup END
         **********************************************************************************************************/

        String  jarFileWithPath  = jarDirectory+topology.getTopologyJarFileName();

        List<String> env = new ArrayList<>();
        env.add("jarFile=" + jarFileWithPath);
        env.add("jarName=" + topology.getTopologyJarFileName());
        env.add("operatorType=" + taskRequestSetupOperator.getOperatorType());
        env.add("operatorInputBrokerIP=" +operatorInputBrokerIP);
        env.add("operatorInputQueue=" +operatorInputFQQN);
        env.add("operatorOutputBrokerIP=" + operatorOutputBrokerIP);
        env.add("operatorOutputAddress=" +operatorOutputAddress);
        env.add("topologyID=" + topology.getTopologyID());
        env.add("taskManagerID=" +taskManagerID);
        env.add("metrics_broker_IP_address=" +metrics_broker_IP_address);
        env.add("metrics_FQQN=" +metrics_FQQN);
        env.add("operatorID=" +taskRequestSetupOperator.getOperator().getOperatorID().getOperatorID_as_String());
        env.add("predecessorInputMethod=" + predecessorSuccessorConfig.getPredecessorInputMethod());
        env.add("predecessorIP=" + predecessorSuccessorConfig.getPredecessorIP());
        env.add("predecessorPort=" + predecessorSuccessorConfig.getPredecessorPort());
        env.add("successorIP=" + predecessorSuccessorConfig.getSuccessorIP());
        env.add("outputTypeToPortMap=" + edgeStreamContainer.getOutputTypeAndPortAsPipedList()); // this list will always have at least one value, the default output port of 8000
        env.add("reconfigurationPlanID=" + reconfigurationPlanID);
        env.add("TMToOPManagementFQQN=" + topology.getTMToOperatorManagementFQQN());
        env.add("OPtoOPManagementFQQN=" + topology.getOperatorToOperatorManagementFQQN());
        env.add("localOperatorMetricsFQQN=" + topology.getOperatorMetricsFQQN());
        env.add("warmupFlag=" + taskRequestSetupOperator.getTaskWarmerStrategy().toString());
        env.add("warmupContainerIP=" + taskWarmerConfig.getWarmUpReplicaTypeAndIP().getRight());
        env.add("warmupContainerOperatorID=" + taskWarmerConfig.getWarmUpReplicaTypeAndIP().getMiddle());
        env.add("notifyPredecessorWhenReady=" + taskRequestSetupOperator.isNotifyPredecessorWhenReady());
        env.add("waitForSuccessorReadyMessage=" + taskRequestSetupOperator.isWaitForSuccessorReadyMessage());





        StringBuilder stateObjectList = new StringBuilder();
        stateObjectList.append("stateObject=");
        stateObjectList.append(routingKeyConfig.getRoutingKey().routingKeyListAsString());
        env.add(stateObjectList.toString());

        HostConfig hostConfig = new HostConfig();
        hostConfig.withMemory(1000000000l * 2l); //2gb memory limit
        hostConfig.withNetworkMode("edgestream");


        CreateContainerCmd containerCMD = dockerClient.createContainerCmd(operatorImage);
        containerCMD.withHostConfig(hostConfig);
        containerCMD.withName(dockerContainerName);
        containerCMD.withEnv(env);
        //containerCMD.withHostName(dockerContainerName);



        /**Start the Docker container**/
        CreateContainerResponse containerResponse = containerCMD.exec();
        dockerClient.startContainerCmd(containerResponse.getId()).exec();
        dockerHost.checkContainerStatus(containerResponse);

        /**Keep track of this Docker container as a managed EdgeStreamContainer */
        //1. get the container id
        String dockerContainerID = containerResponse.getId(); //we can use this id to make api calls to get info about this container
        DockerAPI dockerAPI = new DockerAPI(dockerHost.getDockerHostIP());


        //2. set the identity values for this container
        edgeStreamContainerID.setDockerContainerID(dockerContainerID);
        edgeStreamContainerID.setDockerContainerName(dockerContainerName);


        //3. Set the ip of the newly created container
        edgeStreamContainer.setDockerContainerLocalIP(dockerAPI.getContainerIP(dockerContainerID));
        edgeStreamContainer.setWarmUpStrategy(taskRequestSetupOperator.getTaskWarmerStrategy());


        //4. Add container to the docker host and host manager

        dockerHostManager.addEdgeStreamContainer(edgeStreamContainer,dockerHost); // add to host manager so that it

        //5. remove this operator from the setup queue
        topology.getTopologyReconfigurationManager().dequeOperatorFromSetupQueue(reconfigurationPlanID,taskRequestSetupOperator.getOperatorType());

    }

}
