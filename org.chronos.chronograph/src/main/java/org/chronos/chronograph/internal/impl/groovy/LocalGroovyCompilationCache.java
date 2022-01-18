package org.chronos.chronograph.internal.impl.groovy;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import groovy.lang.Script;

import static com.google.common.base.Preconditions.*;

public class LocalGroovyCompilationCache implements GroovyCompilationCache {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final Cache<String, Class<?extends Script>> localScriptCache = CacheBuilder.newBuilder().maximumSize(255).build();

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public void put(final String scriptContent, final Class<? extends Script> compiledScript) {
        checkNotNull(scriptContent, "Precondition violation - argument 'scriptContent' must not be NULL!");
        checkNotNull(compiledScript, "Precondition violation - argument 'compiledScript' must not be NULL!");
        localScriptCache.put(scriptContent, compiledScript);
    }

    @Override
    public Class<? extends Script> get(final String scriptContent) {
        checkNotNull(scriptContent, "Precondition violation - argument 'scriptContent' must not be NULL!");
        return localScriptCache.getIfPresent(scriptContent);
    }

    @Override
    public void clear() {
        this.localScriptCache.invalidateAll();
    }
}
