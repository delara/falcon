package com.edgestream.worker.operator;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.config.EdgeStreamGetPropertyValues;
import com.edgestream.worker.metrics.metricscollector2.MetricsCollector3;
import com.edgestream.worker.metrics.metricscollector2.reconfiguration.ReconfigurationMeasure;
import com.edgestream.worker.metrics.metricscollector2.statemanagement.StateManagementCollector;
import com.edgestream.worker.metrics.metricscollector2.statemanagement.StateManagementMeasure.OperationType;
import com.edgestream.worker.runtime.reconfiguration.state.AtomicKey;
import com.edgestream.worker.storage.PathstoreClient;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.ArrayList;

public class StatefulOperator extends Operator implements Serializable, SingleTupleProcessing, SingleTupleEmitter {
    public boolean isOperatorMigrating = false;
    public boolean hasReconfigMarkerArrived = false;
    public ArrayList<AtomicKey> routingKeys;
    public StateManagementCollector stateManagementCollector;
    public long changedKeysSize;
    public long backupSize;
    public long backupDuration;
    public long restoreSize;

//    // this constructor is used by WindowOperator
//    public StatefulOperator(OperatorID operatorID, String inputType, MetricsCollector3 metricsCollector) {
//        super(operatorID, inputType, metricsCollector);
//    }

    public StatefulOperator(OperatorID operatorID, String inputType, ArrayList<AtomicKey> routingKeys, MetricsCollector3 metricsCollector) {
        super(operatorID, inputType, metricsCollector);
        this.metricsCollector.setStatefulOperator();
        this.stateManagementCollector = this.metricsCollector.getStateManagementCollector();
        long currentTimestampInMillis = ZonedDateTime.now().toInstant().toEpochMilli();
        this.reconfigurationCollector.add(ReconfigurationMeasure.ReconfigOperation.StartupTime, currentTimestampInMillis);
        isOperatorMigrating = isRestoreRequired(routingKeys);
        System.out.println("Operator is in migrating: " + isOperatorMigrating + " mode");
        this.routingKeys = routingKeys;
        if (!isOperatorMigrating) {
            triggerPeriodicCheckpointing();
        }
    }

    public void triggerPeriodicCheckpointing() {
        new Thread(() -> {
            do {
                try {
                if (EdgeStreamGetPropertyValues.getCheckpointStatus() == "true") {
                    long checkpointStartTime = System.currentTimeMillis();
                    boolean dataTransferred = performCheckpoint();
                    long checkpointEndTime = System.currentTimeMillis();
                    long checkpointDuration = checkpointEndTime - checkpointStartTime;
                    if (dataTransferred) {
                        System.out.println("Execution time for Checkpoint operation: " + checkpointDuration + "ms");
                        this.stateManagementCollector.add(OperationType.Checkpointing, "", backupSize, changedKeysSize, checkpointDuration);
                    }
                }
                    Thread.sleep(10000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } while (true);
        }).start();
    }

    // Each stateful operator can define what it wants to checkpoint
    public boolean performCheckpoint() {return false;}

    public boolean isRestoreRequired(ArrayList<AtomicKey> routingKeys) {
        boolean restoreRequired = false;
        for (AtomicKey routingKey : routingKeys) {
            if (!routingKey.getKey().equals("any")) {
                restoreRequired = true;
                break;
            }
        }
        return restoreRequired;
    }

    @Override
    public void processElement(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
        if (tuple.isReconfigMarker()) {
            long creationTimestampInMillis = ZonedDateTime.parse(timeStamp).toInstant().toEpochMilli();
            this.reconfigurationCollector.add(ReconfigurationMeasure.ReconfigOperation.ReconfigMarker, creationTimestampInMillis);
            if (isOperatorMigrating) {
                // Reconfig marker arrives at the operator replica during migration due to a bug.
                // This is a hacky solution to fix this.
                System.out.println("Reconfiguration request received while operator migration. Will do restore now.");
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // This restores partition keys only if required. (i.e. if operator is in migrating mode.)
                long restoreStartTime = ZonedDateTime.now().toInstant().toEpochMilli();
                performRestore(routingKeys);
                long restoreEndTime = ZonedDateTime.now().toInstant().toEpochMilli();
                long restoreDuration = restoreEndTime - restoreStartTime;
                System.out.println("Execution time for Restore operation: " + restoreDuration + "ms");
                this.stateManagementCollector.add(OperationType.Restore, "", restoreSize, Long.valueOf(routingKeys.size()), restoreDuration);
                //PathstoreClient.eraseBackupSignal();
                hasReconfigMarkerArrived = true;
            } else {
                System.out.println("Reconfiguration request received. Initiating on-demand backup.");
                //System.out.println("Tuple routing keys: " + ReflectionToStringBuilder.toString(tuple.getTupleHeader().getRoutingKeys()));
                //Perform backup operation for only keys to be routed.
                long backupStartTime = System.currentTimeMillis();
                routingKeys = tuple.getTupleHeader().getAtomicKeys();
                performOnDemandBackup(routingKeys);
//                PathstoreClient.signalBackupComplete(getOperatorID().getOperatorID_as_String());
                long backupEndTime = System.currentTimeMillis();
                this.backupDuration = backupEndTime - backupStartTime;
                System.out.println("Execution time for Backup operation: " + backupDuration + "ms");
            }
        } else if (tuple.isStableMarker() && this.isInWarmUpPhase()) {
            long creationTimestampInMillis = ZonedDateTime.parse(timeStamp).toInstant().toEpochMilli();
            this.reconfigurationCollector.add(ReconfigurationMeasure.ReconfigOperation.StableMarker, creationTimestampInMillis);
            System.out.println("[Operator]" + "Stable marker received.....");

            //emit(tuple, tupleID, tupleInternalID, timeStamp, tupleOrigin, inputKey); //bypass the process tuple function and directly emit
            this.disableWarmUpPhase();
            isOperatorMigrating = false;
            this.reset(); //reset the internal state of this operator
            this.enableEmitter(); //allow this operator to start emitting instead of dropping tuples
            this.triggerPeriodicCheckpointing();
        } else {
            // Case 1: If operator is in migrating mode and has already received the reconfigMarker, it can start
            // processing tuples, else they are to be rejected. These rejected tuples could have arrived due to early
            // trigger of dual routing. Case 2: If the operator is not in migrating mode, then for sure it should be
            // processing the tuples, this is the general case.
            if (!isOperatorMigrating || hasReconfigMarkerArrived) {
                collectInputMetrics(tuple);
                processTuple(tuple, tupleID, tupleInternalID, producerId, timeStamp, tupleOrigin, inputKey);
            } else {
                System.out.println("Have not received reconfig marker yet. So will reject the tuple");
            }
        }
    }

    public void performRestore(ArrayList<AtomicKey> routingKeys) {
    }

    public void performOnDemandBackup(ArrayList<AtomicKey> routingKeys) {
    }


    @Override
    public void processTuple(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
    }

    // Reason for overriding emit of Operator class: I am not sure if stateful tuples have the attribute isLiveTuple
    // Probably can get rid of this after testing if the metrics appear similar with removal of the func
    @Override
    public void emit(Tuple tupleToEmit, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, String inputKey) {
        if (tupleToEmit.isLiveTuple()) {
            collectOutputMetrics(tupleToEmit, timeStamp);
        }
        getMessageProducerClient().onRequestToSend(tupleToEmit, tupleID, tupleInternalID, producerId, timeStamp, getOperatorID().getOperatorID_as_String(), tupleOrigin, inputKey, canEmit());
    }
}
