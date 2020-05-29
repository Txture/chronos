package org.chronos.chronograph.internal.impl.structure.graph;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.util.GraphVariableHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.api.transaction.GraphTransactionContext;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.transaction.GraphTransactionContextInternal;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

/**
 * Implementation of the Gremlin {@Link Graph.Variables} interface.
 *
 * <p>
 * This implementation persists the graph variables alongside the actual graph data, i.e.
 * <code>graph.tx().commit()</code> will also commit the graph variables, and <code>graph.tx().rollback()</code> will
 * remove any changes.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public class ChronoGraphVariablesImpl implements org.chronos.chronograph.api.structure.ChronoGraphVariables {

    private final ChronoGraph graph;

    // =====================================================================================================================
    // CONSTRUCTOR
    // =====================================================================================================================

    public ChronoGraphVariablesImpl(final ChronoGraph graph) {
        checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
        this.graph = graph;
    }

    // =====================================================================================================================
    // PUBLIC API
    // =====================================================================================================================

    @Override
    public Set<String> keys() {
        return this.keys(ChronoGraphConstants.VARIABLES_DEFAULT_KEYSPACE);
    }

    @Override
    public Set<String> keys(final String keyspace) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        this.readWrite();
        GraphTransactionContext context = this.getContext();
        ChronoDBTransaction tx = this.getTransaction().getBackingDBTransaction();
        Set<String> keySet = tx.keySet(createChronoDBVariablesKeyspace(keyspace));
        keySet.addAll(context.getModifiedVariables(keyspace));
        keySet.removeAll(context.getRemovedVariables(keyspace));
        return Collections.unmodifiableSet(keySet);
    }


    @Override
    public <R> Optional<R> get(final String key) {
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        return this.getInternal(ChronoGraphConstants.VARIABLES_DEFAULT_KEYSPACE, key);
    }

    @Override
    public <R> Optional<R> get(final String keyspace, final String key) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        return this.getInternal(keyspace, key);
    }

    @SuppressWarnings("unchecked")
    protected <R> Optional<R> getInternal(final String keyspace, final String key) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        this.readWrite();
        GraphTransactionContext context = this.getContext();
        if (context.isVariableModified(keyspace, key)) {
            // return the modified value
            return Optional.ofNullable((R) context.getModifiedVariableValue(keyspace, key));
        } else {
            // query the db for the original value
            ChronoDBTransaction tx = this.getTransaction().getBackingDBTransaction();
            return Optional.ofNullable(tx.get(createChronoDBVariablesKeyspace(keyspace), key));
        }
    }

    @Override
    public void remove(final String key) {
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        this.remove(ChronoGraphConstants.VARIABLES_DEFAULT_KEYSPACE, key);
    }

    @Override
    public void remove(final String keyspace, final String key) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        this.readWrite();
        GraphTransactionContextInternal context = this.getContext();
        context.removeVariable(keyspace, key);
    }


    @Override
    public void set(final String key, final Object value) {
        this.set(ChronoGraphConstants.VARIABLES_DEFAULT_KEYSPACE, key, value);
    }

    @Override
    public void set(final String keyspace, final String key, final Object value) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        GraphVariableHelper.validateVariable(key, value);
        this.readWrite();
        GraphTransactionContextInternal context = this.getContext();
        context.setVariableValue(keyspace, key, value);
    }

    @Override
    public String toString() {
        this.readWrite();
        // unfortunately we HAVE to adhere to the gremlin defaults here, which means
        // that only the default variables keyspace is considered.
        return StringFactory.graphVariablesString(this);
    }

    @Override
    public Map<String, Object> asMap() {
        return this.asMap(ChronoGraphConstants.VARIABLES_DEFAULT_KEYSPACE);
    }

    @Override
    public Map<String, Object> asMap(final String keyspace) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspa' must not be NULL!");
        Set<String> keys = this.keys(keyspace);
        Map<String, Object> resultMap = Maps.newHashMap();
        for (String key : keys) {
            this.get(keyspace, key).ifPresent(value -> resultMap.put(key, value));
        }
        return Collections.unmodifiableMap(resultMap);
    }


    @Override
    public Set<String> keyspaces() {
        this.readWrite();
        Set<String> modifiedKeyspaces = this.getContext().getModifiedVariableKeyspaces();
        ChronoDBTransaction chronoDBTransaction = this.getTransaction().getBackingDBTransaction();
        Set<String> persistedKeyspaces = chronoDBTransaction.keyspaces().stream()
            .filter(keyspace -> keyspace.startsWith(ChronoGraphConstants.KEYSPACE_VARIABLES))
            .map(ChronoGraphVariablesImpl::createChronoGraphVariablesKeyspace)
            .collect(Collectors.toSet());
        return Collections.unmodifiableSet(Sets.union(modifiedKeyspaces, persistedKeyspaces));
    }

    // =====================================================================================================================
    // INTERNAL HELPER METHODS
    // =====================================================================================================================

    protected ChronoGraphTransaction getTransaction() {
        return this.graph.tx().getCurrentTransaction();
    }

    protected void readWrite() {
        this.graph.tx().readWrite();
    }

    protected GraphTransactionContextInternal getContext() {
        return (GraphTransactionContextInternal) this.getTransaction().getContext();
    }

    public static String createChronoDBVariablesKeyspace(String chronoGraphKeyspace) {
        checkNotNull(chronoGraphKeyspace, "Precondition violation - argument 'chronoGraphKeyspace' must not be NULL!");
        if(ChronoGraphConstants.VARIABLES_DEFAULT_KEYSPACE.equals(chronoGraphKeyspace)){
            // for backwards compatibility with earlier releases where only the default
            // chronoGraphKeyspace existed, we do not attach any suffix to it.
            return ChronoGraphConstants.KEYSPACE_VARIABLES;
        }else{
            return ChronoGraphConstants.KEYSPACE_VARIABLES + "#" + chronoGraphKeyspace;
        }
    }

    public static String createChronoGraphVariablesKeyspace(String chronoDBKeyspace){
        checkNotNull(chronoDBKeyspace, "Precondition violation - argument 'chronoDBKeyspace' must not be NULL!");
        if(ChronoGraphConstants.KEYSPACE_VARIABLES.equals(chronoDBKeyspace)){
            return ChronoGraphConstants.VARIABLES_DEFAULT_KEYSPACE;
        }
        String prefix = ChronoGraphConstants.KEYSPACE_VARIABLES + "#";
        if(chronoDBKeyspace.startsWith(prefix)){
            return chronoDBKeyspace.substring(prefix.length());
        }else{
            throw new IllegalArgumentException("The given 'chronoDBKeyspace' is not a syntactically valid keyspace identifier: '" + chronoDBKeyspace + "'");
        }
    }
}
