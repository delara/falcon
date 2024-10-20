# Merlin Worker

##########################################################
## Build Image                                           #
##########################################################

#docker build --no-cache -t brianr/merlinworker:latest .
#git fetch origin ; git checkout master ;git merge origin/master
#docker run -i brianr/merlinworker W003 192.168.86.35  W001 10.10.10.9

#docker run -i brianr/merlinworker W001 10.70.20.196 W001 10.70.2.109
#docker run -i brianr/merlinworker W002 10.70.20.47 W001 10.70.2.109
#docker run -i brianr/merlinworker W003 10.70.20.48 W001 10.70.2.109

FROM openjdk:11-jdk as intermediate

# install git
RUN apt-get update
RUN apt-get install -y git


COPY merlin.key /
# add credentials on build



#Clone merlin github URL
RUN chmod 400 merlin.key \
    && eval `ssh-agent -s` \
    && ssh-add merlin.key \
    && mkdir /root/.ssh/  \
    &&  touch /root/.ssh/known_hosts  \
    && chmod go-w /root  \
    && chmod 700 /root/.ssh  \
    &&  ssh-keyscan github.com >> /root/.ssh/known_hosts \
    && git clone git@github.com:delara/edgestream.git





FROM openjdk:11-jdk
# copy the repository form the previous image
COPY --from=intermediate /edgestream /srv/edgestream



#Run maven install to build worker.jar
RUN wget https://apache.mirror.globo.tech/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz \
        && tar xvf apache-maven-3.6.3-bin.tar.gz \
        && mv apache-maven-3.6.3 /opt/

ENV PATH="/opt/apache-maven-3.6.3/bin:${PATH}"
RUN echo $PATH

WORKDIR /srv/edgestream/Worker/
RUN mvn -T 1C clean install


#java -jar Worker.jar W003 10.70.20.48 W001 10.70.2.109


ENTRYPOINT ["java","-jar","/srv/edgestream/Worker/target/Worker.jar"]
CMD ["$task_manager_id","$broker_ip","$parent_taskmanager_id","$cluster_manager_ip","$platform","$parent_taskmanager_ip"]