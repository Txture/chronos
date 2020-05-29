package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import org.chronos.chronograph.api.structure.ChronoGraphVariables;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class ReadOnlyVariables implements ChronoGraphVariables {

    private final ChronoGraphVariables variables;

    public ReadOnlyVariables(final ChronoGraphVariables variables) {
        checkNotNull(variables, "Precondition violation - argument 'variables' must not be NULL!");
        this.variables = variables;
    }

    @Override
    public Set<String> keys() {
        return Collections.unmodifiableSet(this.variables.keys());
    }

    @Override
    public Set<String> keys(final String keyspace) {
        return Collections.unmodifiableSet(this.variables.keys(keyspace));
    }

    @Override
    public <R> Optional<R> get(final String key) {
        return this.variables.get(key);
    }

    @Override
    public <R> Optional<R> get(final String keyspace, final String key) {
        return this.variables.get(keyspace, key);
    }

    @Override
    public void set(final String key, final Object value) {
        this.unsupportedOperation();
    }

    @Override
    public void set(final String keyspace, final String key, final Object value) {
        this.unsupportedOperation();
    }

    @Override
    public void remove(final String key) {
        this.unsupportedOperation();
    }

    @Override
    public void remove(final String keyspace, final String key) {
        this.unsupportedOperation();
    }

    @Override
    public Map<String, Object> asMap() {
        return Collections.unmodifiableMap(this.variables.asMap());
    }

    @Override
    public Map<String, Object> asMap(final String keyspace) {
        return Collections.unmodifiableMap(this.variables.asMap(keyspace));
    }

    @Override
    public Set<String> keyspaces() {
        return Collections.unmodifiableSet(this.variables.keyspaces());
    }

    private void unsupportedOperation() {
        throw new UnsupportedOperationException("This operation is not supported for readOnly graph!");
    }
}
