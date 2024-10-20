package com.edgestream.worker.runtime.plan;

/*
v1 done
 */


public class Single_Node_Resource {
    private final String task_manager_id;
    private int current_connections;
    private int current_queues;
    private int current_operators;
    private final int max_connections;
    private final int max_queues;
    private final int max_operators;

    public void setCurrent_connections(int current_connections) {
        this.current_connections = current_connections;
    }

    public void setCurrent_queues(int current_queues) {
        this.current_queues = current_queues;
    }

    public void setCurrent_operators(int current_operators) {
        this.current_operators = current_operators;
    }

    public String getTask_manager_id() {
        return task_manager_id;
    }

    public int getCurrent_connections() {
        return current_connections;
    }

    public int getCurrent_queues() {
        return current_queues;
    }

    public int getCurrent_operators() {
        return current_operators;
    }

    public int getMax_connections() {
        return max_connections;
    }

    public int getMax_queues() {
        return max_queues;
    }

    public int getMax_operators() {
        return max_operators;
    }

    public Single_Node_Resource(String task_manager_id, int max_connections, int max_queues, int max_operators) {
        this.task_manager_id = task_manager_id;
        this.max_connections = max_connections;
        this.max_queues = max_queues;
        this.max_operators = max_operators;
        this.current_connections = 0;
        this.current_queues = 0;
        this.current_operators = 0;
    }



}
