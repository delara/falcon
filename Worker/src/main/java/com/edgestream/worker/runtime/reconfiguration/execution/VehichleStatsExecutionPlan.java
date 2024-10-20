package com.edgestream.worker.runtime.reconfiguration.execution;

import com.edgestream.worker.config.EdgeStreamGetPropertyValues;
import com.edgestream.worker.runtime.docker.DockerFileType;
import com.edgestream.worker.runtime.plan.Host;
import com.edgestream.worker.runtime.plan.OperatorInPPO;
import com.edgestream.worker.runtime.reconfiguration.ReconfigurationPlan;
import com.edgestream.worker.runtime.reconfiguration.common.DagElementType;
import com.edgestream.worker.runtime.reconfiguration.common.DagPlan;
import com.edgestream.worker.runtime.task.model.TaskType;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.ArrayList;
import java.util.List;

public class VehichleStatsExecutionPlan extends ExecutionPlan {

    public VehichleStatsExecutionPlan() {}

    @Override
    public void createReconfigurationPlan(ExecutionPlanConfig executionPlanConfig) {

        String reconfigurationPlanID = executionPlanConfig.getReconfigurationPlanID();
        String topologyID = executionPlanConfig.getTopologyID();
        List<Host> destinationDataCenterTaskManagers = executionPlanConfig.getDestinationDataCenterTaskManagers();
        DagPlan dagPlan = executionPlanConfig.getDagPlan();
        DockerFileType dockerFileType =executionPlanConfig.getDockerFileType();
        TaskType taskType = executionPlanConfig.getTaskType();


        /**Create Operator(s) */
        List<OperatorInPPO> OperatorsInPPO = new ArrayList<>();

        /**Create Operators**/
        //1. define the dag
        ArrayList<ImmutablePair> dag = new ArrayList<>();
        dag.add(new ImmutablePair<>("V","D"));
        dag.add(new ImmutablePair<>("D","NONE"));

        //Set the member on the superclass so we can access it later
        this.setDag(dag);



        this.setDagPlan(dagPlan);

        for (ImmutablePair<String, Integer> dagElement: dagPlan.getDagElements()) {


            System.out.println("DagElement " + dagElement.left);


            String inputType = dagElement.left;
            String Filepath = EdgeStreamGetPropertyValues.getVEH_STATS_APP_PATH();

            ArrayList<String> destinationTiers = getDestinationTiers(destinationDataCenterTaskManagers);

            //search the dag and find the predecessors and successors
            ArrayList<String> Predecessor_IDs = getPredecessors(inputType,dag);
            ArrayList<String> Successor_IDs = getSuccessors(inputType,dag);


            if(taskType == TaskType.CREATE) {
                OperatorInPPO operator = null;

                if(dagPlan.isDagElementStateful(dagElement)){
                    operator = this.createOperator(inputType, Filepath, destinationTiers, Predecessor_IDs, String.valueOf(dagElement.right), Successor_IDs, DagElementType.STATEFUL);
                    operator.addStateTransferPlan(dagPlan.getElementStateTransferPlan(dagElement));

                }else{
                    operator = this.createOperator(inputType, Filepath, destinationTiers, Predecessor_IDs, String.valueOf(dagElement.right), Successor_IDs, DagElementType.STATELESS);
                }

                OperatorsInPPO.add(operator);
            }




            if(taskType == TaskType.REMOVE) {

                OperatorInPPO operator = null;

                if(dagPlan.isDagElementStateful(dagElement)){
                    operator = this.removeOperator(inputType, Filepath, destinationTiers, Predecessor_IDs, String.valueOf(dagElement.right), Successor_IDs, DagElementType.STATEFUL);
                    operator.addStateTransferPlan(dagPlan.getElementStateTransferPlan(dagElement));
                }else{
                    operator = this.removeOperator(inputType, Filepath, destinationTiers, Predecessor_IDs, String.valueOf(dagElement.right), Successor_IDs, DagElementType.STATELESS);
                }

                OperatorsInPPO.add(operator);

            }


        }


        this.setReconfigurationPlan(new ReconfigurationPlan(reconfigurationPlanID,topologyID,destinationDataCenterTaskManagers,OperatorsInPPO, dockerFileType, dag));
        this.setDockerFileType(dockerFileType);
        this.setExecutionType(executionPlanConfig.getExecutionType());

    }
}
