# Merlin Operator

##########################################################
## Build Image                                           #
##########################################################


#docker build --no-cache -t brianr/merlinoperator:latest .

#git fetch origin ; git checkout master ;git merge origin/master

#docker run -i brianr/merlinworker W003 192.168.86.35  W001 10.10.10.9

#docker run -i brianr/merlinworker W001 10.70.20.196 W001 10.70.2.109
#docker run -i brianr/merlinworker W002 10.70.20.47 W001 10.70.2.109
#docker run -i brianr/merlinworker W003 10.70.20.48 W001 10.70.2.109

FROM openjdk:11-jdk as intermediate

# install git
RUN apt-get update
RUN apt-get install -y git
#ARG jarFile
#copy the jar file from local filesystem to edgeStreamContainer root dir
WORKDIR /
COPY $jarFile /app
#RUN ["ls"]
WORKDIR /app

CMD java -jar /app/$jarName $jarParams $operatorInputBrokerIP $operatorInputQueue $operatorOutputBrokerIP $operatorOutputAddress $topologyID $node_ID $metrics_broker_IP_address $metrics_FQQN $operatorID
