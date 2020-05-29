package org.chronos.chronograph.api.transaction.trigger;

import org.chronos.common.exceptions.UnknownEnumLiteralException;

public enum TriggerTiming {

    /**
     * Triggers with this timing will be fired <b>before</b> an actual commit occurs, <b>before</b> the state merge with the store and <b>before</b> the commit lock has been acquired.
     *
     * <p>
     * This is the earliest trigger timing. The {@linkplain TriggerContext#getCurrentState() current transaction state} is passed as-is from the user (it may have been modified by other {@link #PRE_COMMIT} triggers though).
     * </p>
     *
     * <p>
     * Triggers in this timing <b>can</b> cancel the commit by throwing a {@link CancelCommitException}.
     * </p>
     *
     * <p>
     * Triggers in this timing <b>can</b> modify the {@linkplain TriggerContext#getCurrentState() current state} of the transaction.
     * </p>
     */
    PRE_COMMIT,

    /**
     * Triggers with this timing will be fired <b>before</b> an actual commit occurs, <b>after</b> the state merge with the store and <b>after</b> the commit lock has been acquired.
     *
     * <p>
     * This trigger timing occurs after {@link #PRE_COMMIT} and before {@link #POST_PERSIST}. This is the latest timing where the {@linkplain TriggerContext#getCurrentState() current transaction state} can be modified. The current transaction state will be the result of the merge / conflict resolution between the transaction and the {@link TriggerContext#getStoreState() current store state}.
     * </p>
     *
     * <p>
     * Triggers in this timing <b>can</b> cancel the commit by throwing a {@link CancelCommitException}.
     * </p>
     *
     * <p>
     * Triggers in this timing <b>can</b> modify the {@linkplain TriggerContext#getCurrentState() current state} of the transaction.
     * </p>
     */
    PRE_PERSIST,


    /**
     * Triggers with this timing will be fired <b>after</b> an actual commit occurs and <b>before</b> the commit lock has been released.
     *
     * <p>
     * This trigger timing occurs after {@link #PRE_PERSIST} and before {@link #POST_COMMIT}. This is the first timing after the {@linkplain TriggerContext#getCurrentState() current transaction state} has been committed. The current transaction state will be the result of the merge / conflict resolution between the transaction and the {@link TriggerContext#getStoreState() current store state}.
     * </p>
     *
     * <p>
     * Triggers in this timing <b>can not</b> cancel the commit by throwing a {@link CancelCommitException}.
     * </p>
     *
     * <p>
     * Triggers in this timing <b>can not</b> modify the {@linkplain TriggerContext#getCurrentState() current state} of the transaction.
     * </p>
     */
    POST_PERSIST,

    /**
     * Triggers with this timing will be fired <b>after</b> an actual commit occurs and <b>after</b> the commit lock has been released.
     *
     * <p>
     * This trigger timing occurs after {@link #POST_PERSIST} and is the latest possible trigger timing.
     * </p>
     *
     * <p>
     * Triggers in this timing <b>can not</b> cancel the commit by throwing a {@link CancelCommitException}.
     * </p>
     *
     * <p>
     * Triggers in this timing <b>can not</b> modify the {@linkplain TriggerContext#getCurrentState() current state} of the transaction.
     * </p>
     */
    POST_COMMIT;

    public boolean isBeforePersist() {
        switch (this) {
            case PRE_COMMIT:
                return true;
            case PRE_PERSIST:
                return true;
            case POST_PERSIST:
                return false;
            case POST_COMMIT:
                return false;
            default:
                throw new UnknownEnumLiteralException(this);
        }
    }

    public boolean isAfterPersist() {
        return !this.isBeforePersist();
    }

}
