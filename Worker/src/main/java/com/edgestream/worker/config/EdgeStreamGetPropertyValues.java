package com.edgestream.worker.config;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

public class EdgeStreamGetPropertyValues {


    private static final Properties props = new Properties();

    public static String getCheckpointStatus() {
        return props.getProperty("CHECKPOINT_FEATURE");
    }

    public static void setCheckpointStatus(String status) {
        props.setProperty("CHECKPOINT_FEATURE", status);
    }

    public static String getCheckpointFrequency() {
        return props.getProperty("CHECKPOINT_FREQUENCY");
    }

    /****************************************
     *
     *
     * Worker Properties
     *
     *
     **************************************/


    public static String getCLUSTER_MANAGER_IP(){
        loadPropValuesFromFile();
        return props.getProperty("CLUSTER_MANAGER_IP");
    }


    public static String getCLUSTER_MANAGER_PORT() {
        loadPropValuesFromFile();
        return props.getProperty("CLUSTER_MANAGER_PORT");
    }

    public static String getCLOUD_ROOT_DATACENTER_IP(){
        loadPropValuesFromFile();
        return props.getProperty("CLOUD_ROOT_DATACENTER_IP");
    }

    public static String getEDGE1_DATACENTER_IP(){
        loadPropValuesFromFile();
        return props.getProperty("EDGE1_DATACENTER_IP");
    }

    public static String getEDGE2_DATACENTER_IP(){
        loadPropValuesFromFile();
        return props.getProperty("EDGE2_DATACENTER_IP");
    }

    public static String getTASK_MANAGER_WORKING_FOLDER(){
        loadPropValuesFromFile();
        return props.getProperty("TASK_MANAGER_WORKING_FOLDER");
    }

    public static String getNETWORK_LAYOUT(){
        loadPropValuesFromFile();
        return props.getProperty("NETWORK_LAYOUT");
    }


    public static String getMETRICS_LOG_PATH(){
        loadPropValuesFromFile();
        return props.getProperty("METRICS_LOG_PATH");
    }

    public static String getVEH_STATS_APP_PATH(){
        loadPropValuesFromFile();
        return props.getProperty("VEH_STATS_APP_PATH");
    }


    /*************************************
     *
     * Docker Host IP look up
     *
     *
     **************************************/


    public static HashMap<String,String> getListOfDatacenterDockerHosts(String taskManagerID){

        loadPropValuesFromFile();

        HashMap<String,String> dockerHostInfo = new HashMap<>();
        boolean endOfList = false;
        String confType = "docker.host.info.";

        int i = 0;
        while(!endOfList){
            if(props.containsKey(confType + taskManagerID+ "." + i)){
                String dockerHostIP = props.get(confType + taskManagerID+ "." + i).toString().split(",")[0];
                String scheduledHostID = props.get(confType + taskManagerID+ "." + i).toString().split(",")[1];
                dockerHostInfo.put(dockerHostIP,scheduledHostID);
                i++;
                System.out.println("Found docker host: [" + dockerHostIP + "] and scheduledHostID: [" + scheduledHostID + "]" );
            }else{
                endOfList= true;
                //i = i -1;
                System.out.println("Reach end of list, found this many IPs in the properties file for TaskManager: [" + taskManagerID + "] IP count:[" + i +"]" );
            }
        }

        return dockerHostInfo;
    }


















    /**********************************************************************************************************
     * (re)load the values from the file
     * Do not modify anything below
     *
     * ************************************************************************************************/
    public static void EdgeStreamGetPropertyValues(){

        loadPropValuesFromFile();

    }



    private static String loadPropValuesFromFile()  {
        String result = "";
        InputStream inputStream = null;
        try {

            String propFileName = "config.worker.properties";

            //System.out.println("Reading properties file: " + propFileName);

            inputStream = EdgeStreamGetPropertyValues.class.getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                props.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }


        } catch (Exception e) {
            System.out.println("Exception: " + e);
        } finally {
            try{
                inputStream.close();
            }catch (Exception e){
                System.out.println("Could not close file");

            }

        }
        return result;
    }
}