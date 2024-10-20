# Falcon: Live Reconfiguration for Stateful Stream Processing on the Edge

Falcon is a distributed stream processing framework that supports live reconfiguration of stateful applications on the
hierachical edge-cloud environment.

You can find more information about this system in our paper: [Link coming soon!]().

The setup of this project is split into 6 important parts -

1. Network: The network is the collection of all the servers that are connected to the framework. The network is managed
   by the client.
2. Client: The client is persona of the user who is deploying the framework on a remote server.
3. Workers: Instances of the framework that are deployed on remote servers.
4. Database: The database is used to store the state of the operators in the framework.
5. Application: The application that is being deployed on the workers.
6. Data producer: The data producer is the source of the data that is being processed by the application.

## Section 1: Network Setup

1. The current network topology assumes a 2-tier architecture with one cloud and two edge nodes. Hence, you will need to
   have 4 machines (one extra for `ClusterManager`) for such a deployment.
2. You will need to change the IP addresses in the config file to match your machine's IP addresses. You can use the
   same IP address for the docker hosts.
3. If you want to change the network topology, please follow the instructions in the readme of the `Network` class.

## Section 2: Client Setup

1. Clone this repo and set it up on an IDE like Jetbrains Intellij Idea or Microsoft Visual Studio Code.
2. You should use Java SDK 11.0.x for this project.
3. You also need to have Maven installed and configured on your system. (Tested with version 3.8.5)
4. For triggering the deployment of the application on the framework servers (i.e. workers), you need to run
   the `FalconClientManager` class.

## Section 3: Worker Setup

### Pre-requisites

1. Docker should be installed on the machine. Falcon's deploys applications as docker containers.
2. You will also need to install [ActiveMQ Artemis](https://activemq.apache.org/components/artemis/) on the machine.
   Falcon uses ActiveMQ Artemis for routing tuples.
3. After successful installation, will also need to update the Artemis broker config. Specifically, you need to modify
   the `broker.xml`. We have provided a reference in `Worker/broker_config`. Make sure the rest of the files in
   the `etc` directory are also following similar pattern as those provided by us as reference.

### Steps

1. Build the Falcon project on the client machine using the command `mvn clean install`.
2. On a successful build, you should have a `target` directory in the `Worker` directory.
3. You will need to upload the `Worker.jar` file present in the `target` directory to the remote server.
4. You will also need to create a `jarz` directory in the remote server and update the `TASK_MANAGER_WORKING_FOLDER`
   property in the client's config file with this path.
5. To start the workers and brokers on all machines, you can run the `Start.sh` script present in
   the `Worker/StartUpScripts` directory. This script in turn calls the `StartBrokers.sh` and `StartWorkers.sh` scripts.
6. To stop the workers and brokers on all machines, you can run the `Kill.sh` script present in the
   `Worker/StartUpScripts` directory. This script in turn calls the `DockerCleanUp.sh`, `KillBrokers.sh`
   and `KillWorkers.sh` scripts.
7. You should make sure that IP addresses are correctly reflected for all these scripts. We recommend creating an
   additional machine (in addition to the 4 machines in Section 1.1) for running these scripts and managing the
   deployment of workers and brokers.

## Section 4: Database Setup

1. Falcon uses an open-sourced geo-distributed database called Pathstore. You can find the source code for Pathstore
   [here](https://github.com/PathStore/pathstore-all).
2. You will need to deploy Pathstore on all the worker machines in the network. You can follow the instructions in the
   Pathstore repository to deploy the database. You should create the same hierarchy as you have here in Falcon (e.g.,
   one cloud, two edges).

## Section 5: Application Setup

1. We have provided an example application called "VehicleStats". This is already configured to be deployed on the
   Falcon framework.
2. After building this application, you will have a `target` directory in the `Application` directory. You need to
   update the `VEH_STATS_APP_PATH` property in the config file with this path.
3. This application has two operators of type V and D. The V operator is a stateful operator that maintains a running
   average of each vehicle and the D operator writes data to a CSV file.
4. This CSV file can be extracted to plot performance metrics of the application and the framework.
5. In this application, we have also provided more examples of operators such as a stateless operator, window operator,
   operator maintaining large state, etc.
6. You can modify the `VehicleStatsAggregator` class to change the application logic and add or remove operators.

## Section 6: Data Producer Setup

1. We have provided the data producer for the VehicleStats application in the `testing` directory of the application.
2. After building the project, you should see `VehicleStatsTupleProducer.jar` in the `target` directory of the
   application.
3. You can run this jar file to start producing data for the application. The run command should look something like
   `java -jar VehicleStatsTupleProducer.jar V tcp://<EDGE_BROKER_IP>:61616 merlin_default vehstats001 W003 <PRODUCTION_RATE> P6`
   .
4. You can refer to the `GeneralProducer` class for more information about the runtime arguements.