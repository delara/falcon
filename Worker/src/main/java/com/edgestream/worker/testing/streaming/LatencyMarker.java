package com.edgestream.worker.testing.streaming;

import com.edgestream.worker.operator.OperatorID;

public final class LatencyMarker extends StreamElement {

    // ------------------------------------------------------------------------

    /** The time the latency mark is denoting. */
    private final long markedTime;

    private final OperatorID operatorId;

    private final int subtaskIndex;

    /**
     * Creates a latency mark with the given timestamp.
     */
    public LatencyMarker(long markedTime, OperatorID operatorId, int subtaskIndex) {
        this.markedTime = markedTime;
        this.operatorId = operatorId;
        this.subtaskIndex = subtaskIndex;
    }

    /**
     * Returns the timestamp marked by the LatencyMarker.
     */
    public long getMarkedTime() {
        return markedTime;
    }

    public OperatorID getOperatorId() {
        return operatorId;
    }

    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()){
            return false;
        }

        LatencyMarker that = (LatencyMarker) o;

        if (markedTime != that.markedTime) {
            return false;
        }
        if (!operatorId.equals(that.operatorId)) {
            return false;
        }
        return subtaskIndex == that.subtaskIndex;

    }

    @Override
    public int hashCode() {
        int result = (int) (markedTime ^ (markedTime >>> 32));
        result = 31 * result + operatorId.hashCode();
        result = 31 * result + subtaskIndex;
        return result;
    }

    @Override
    public String toString() {
        return "LatencyMarker{" +
                "markedTime=" + markedTime +
                ", operatorId=" + operatorId +
                ", subtaskIndex=" + subtaskIndex +
                '}';
    }
}