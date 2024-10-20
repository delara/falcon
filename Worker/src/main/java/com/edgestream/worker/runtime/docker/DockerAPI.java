package com.edgestream.worker.runtime.docker;

import com.edgestream.worker.runtime.container.EdgeStreamContainer;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class DockerAPI {

    String dockerHostIP;

    public DockerAPI(String dockerHostIP ) {
        this.dockerHostIP = dockerHostIP;

    }


    private JSONObject getDockerContainerDetails(String dockerContainerID) throws IOException {

        String URL = "http://" + dockerHostIP  + ":2375/containers/" + dockerContainerID+"/json";

        JSONObject json = new JSONObject(IOUtils.toString(new URL(URL), StandardCharsets.UTF_8));

        return json;

    }



    public JSONObject getDockerContainerDetails(EdgeStreamContainer edgeStreamContainer) throws IOException {

        String URL = "http://" + dockerHostIP  + ":2375/containers/" + edgeStreamContainer.getEdgeStreamContainerID().getDockerContainerID()+"/json";

        JSONObject json = new JSONObject(IOUtils.toString(new URL(URL), StandardCharsets.UTF_8));

        return json;

    }


    public String getContainerIP(String dockerContainerID){

        String containerIP = null;
        try {
            containerIP = filterDockerContainerInfoIP(getDockerContainerDetails(dockerContainerID));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return containerIP;

    }

    public String filterDockerContainerInfoIP(JSONObject json){

        //String containerIP = new JSONObject(json.get("NetworkSettings").toString()).get("IPAddress").toString();


        JSONObject  networkSettings = new JSONObject(json.get("NetworkSettings").toString());
        JSONObject  Networks = new JSONObject(networkSettings.get("Networks").toString());
        JSONObject networkINFO = Networks.getJSONObject("edgestream");
        String containerIP = networkINFO.getString("IPAddress");
        return containerIP;
    }
}
