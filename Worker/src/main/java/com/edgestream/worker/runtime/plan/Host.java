package com.edgestream.worker.runtime.plan;

public class Host {

    private final String Name;
    private final String Task_Manager_ID;
    private final String Type;
    private final String Host_Address;
    private final String Host_Port;



    public Host(String name, String task_Manager_ID, String type, String host_Address, String host_Port) {
        Name = name;
        Task_Manager_ID = task_Manager_ID;
        Type = type;
        Host_Address = host_Address;
        Host_Port = host_Port;
    }



    public String getName() {
        return Name;
    }

    public String getTask_Manager_ID() {
        return Task_Manager_ID;
    }

    public String getType() {
        return Type;
    }

    public String getHost_Address() {
        return Host_Address;
    }

    public String getHost_Port() {
        return Host_Port;
    }

}
