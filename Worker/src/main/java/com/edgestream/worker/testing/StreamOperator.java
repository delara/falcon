package com.edgestream.worker.testing;


import com.edgestream.worker.operator.OperatorID;
import com.edgestream.worker.testing.streaming.StreamRecord;

import java.io.Serializable;

/**
 * Basic interface for stream operators. Implementers would implement one of
 //* {@link org.apache.flink.streaming.api.operators.OneInputStreamOperator} or
 * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator} to create operators
 * that process elements.
 *
 * <p>The class {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator}
 * offers default implementation for the lifecycle and properties methods.
 *
 * <p>Methods of {@code StreamOperator} are guaranteed not to be called concurrently. Also, if using
 * the timer service, timer callbacks are also guaranteed not to be called concurrently with
 * methods on {@code StreamOperator}.
 *
 * @param <OUT> The output type of the starstream.operator
 */

public interface StreamOperator<OUT> extends Serializable {

    // ------------------------------------------------------------------------
    //  life cycle
    // ------------------------------------------------------------------------

    /**
     * This method is called immediately before any elements are processed, it should contain the
     * starstream.operator's initialization logic.
     *
     * @implSpec In case of recovery, this method needs to ensure that all recovered data is processed before passing
     * back control, so that the order of elements is ensured during the recovery of an starstream.operator chain (operators
     * are opened from the tail starstream.operator to the head starstream.operator).
     *
     * @throws java.lang.Exception An exception in this method causes the starstream.operator to fail.
     */
    void open() throws Exception;

    /**
     * This method is called after all records have been added to the operators via the methods
     * {@link org.apache.flink.streaming.api.operators.OneInputStreamOperator#processElement(StreamRecord)}, or
     * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator#processElement1(StreamRecord)} and
     * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator#processElement2(StreamRecord)}.
     *
     * <p>The method is expected to flush all remaining buffered data. Exceptions during this
     * flushing of buffered should be propagated, in order to cause the operation to be recognized
     * as failed, because the last data items are not processed properly.
     *
     * @throws java.lang.Exception An exception in this method causes the starstream.operator to fail.
     */
    void close() throws Exception;

    /**
     * This method is called at the very end of the starstream.operator's life, both in the case of a successful
     * completion of the operation, and in the case of a failure and canceling.
     *
     * <p>This method is expected to make a thorough effort to release all resources
     * that the starstream.operator has acquired.
     */

    void dispose();



    /**
     * Provides a context to initialize all state in the starstream.operator.
     */
    void initializeState() throws Exception;

    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------

    void setKeyContextElement1(StreamRecord<?> record) throws Exception;




    OperatorID getOperatorID();
}