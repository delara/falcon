package com.edgestream.worker.runtime.docker;

public class DockerFile {


    String dockerFileImageContent;
    DockerFileType dockerFileType;
    String dockerFilePath;


    public DockerFile(String dockerFileImageContent, DockerFileType dockerFileType, String dockerFilePath) {

        this.dockerFileImageContent = dockerFileImageContent;
        this.dockerFileType = dockerFileType;
        this.dockerFilePath = dockerFilePath;
    }

    public String getDockerFileImageContents() {
        return dockerFileImageContent;
    }


    public DockerFileType getDockerFileType() {
        return dockerFileType;
    }

    public String getDockerFilePath() {
        return dockerFilePath;
    }

}
