package org.chronos.chronodb.test.cases.util.model.payload;

import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.common.test.utils.NamedPayload;

import java.util.Collections;
import java.util.Set;

public class NamedPayloadNameIndexer implements StringIndexer {

    private final boolean toLowerCase;

    public NamedPayloadNameIndexer() {
        this(false);
    }

    public NamedPayloadNameIndexer(final boolean toLowerCase) {
        this.toLowerCase = toLowerCase;
    }

    @Override
    public boolean canIndex(final Object object) {
        return object != null && object instanceof NamedPayload;
    }

    @Override
    public Set<String> getIndexValues(final Object object) {
        NamedPayload payload = (NamedPayload) object;
        String name = payload.getName();
        if (this.toLowerCase) {
            return Collections.singleton(name.toLowerCase());
        } else {
            return Collections.singleton(name);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        NamedPayloadNameIndexer that = (NamedPayloadNameIndexer) o;

        return toLowerCase == that.toLowerCase;
    }

    @Override
    public int hashCode() {
        return (toLowerCase ? 1 : 0);
    }
}