package org.chronos.chronodb.internal.impl.engines.base;

import static com.google.common.base.Preconditions.*;

public class ChronosInternalCommitMetadata {

    private Object payload;

    protected ChronosInternalCommitMetadata(){
        // default constructor for (de-)serialization
    }

    public ChronosInternalCommitMetadata(Object payload){
        checkNotNull(payload, "Precondition violation - argument 'payload' must not be NULL!");
        this.payload = payload;
    }

    public Object getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "ChronosInternalCommitMetadata[" + this.payload + "]";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ChronosInternalCommitMetadata that = (ChronosInternalCommitMetadata) o;

        return payload != null ? payload.equals(that.payload) : that.payload == null;
    }

    @Override
    public int hashCode() {
        return payload != null ? payload.hashCode() : 0;
    }
}
