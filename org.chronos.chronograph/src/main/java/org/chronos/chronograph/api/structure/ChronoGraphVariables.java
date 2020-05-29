package org.chronos.chronograph.api.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface ChronoGraphVariables extends Graph.Variables {

    /**
     * Gets the key set of the <b>default</b> graph variables keyspace.
     */
    public Set<String> keys();

    /**
     * Gets the key set of the given graph variables keyspace.
     *
     * @param keyspace The keyspace to get the keys for. Must not be <code>null</code>.
     * @return The key set. Will be empty if the keyspace is unknown. Never <code>null</code>.
     */
    public Set<String> keys(String keyspace);

    /**
     * Gets a variable from the <b>default</b> graph variables keyspace.
     *
     * @param key The key to get the value for.
     * @return The value, as an {@link Optional}. Will never be <code>null</code>, but may be {@linkplain Optional#isPresent() absent} if the variable doesn't exist.
     */
    public <R> Optional<R> get(final String key);

    /**
     * Gets a variable from the given graph variables keyspace.
     *
     * @param keyspace The keyspace in which to search for the key. Must not be <code>null</code>.
     * @param key      The key to get the value for. Must not be <code>null</code>.
     * @param <R>      The expected type of the value.
     * @return The value, as an {@link Optional}. Will never be <code>null</code>, but may be {@linkplain Optional#isPresent() absent} if the variable doesn't exist.
     */
    public <R> Optional<R> get(final String keyspace, final String key);

    /**
     * Sets a variable in the <b>default</b> graph variables keyspace.
     * <p>
     * Any previous value will be silently overwritten.
     *
     * @param key   The variable to set. Must not be <code>null</code>.
     * @param value The value to assign to the variable. Must not be <code>null</code>. If you wish to clear a variable, use {@link #remove(String)} instead.
     * @see #set(String, String, Object)
     */
    public void set(final String key, Object value);

    /**
     * Sets a variable in the given keyspace to the given value.
     *
     * @param keyspace The keyspace of the variable to set. Must not be <code>null</code>.
     * @param key      The key to set the value for. Must not be <code>null</code>.
     * @param value    The value to assign. Must not be <code>null</code>. If you wish to clear a variable, use {@link #remove(String, String)} instead.
     * @see #set(String, Object)
     */
    public void set(final String keyspace, final String key, final Object value);

    /**
     * Removes a variable from the <b>default</b> graph variables keyspace.
     */
    public void remove(final String key);

    /**
     * Removes a variable from the given keyspace.
     *
     * @param keyspace The keyspace of the variable to remove. Must not be <code>null</code>.
     * @param key      The key to remove. Must not be <code>null</code>.
     */
    public void remove(final String keyspace, final String key);

    /**
     * Gets the variables of the <b>default</b> variables keyspace as a {@code Map}.
     */
    public Map<String, Object> asMap();

    /**
     * Gets the variables of the given variables keyspace as a {@link Map}.
     *
     * @param keyspace
     * @return
     */
    public Map<String, Object> asMap(String keyspace);

    /**
     * Gets the set of known variables keyspaces.
     *
     * @return The set of known keyspaces.
     */
    public Set<String> keyspaces();

}
