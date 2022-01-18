package org.chronos.chronograph.internal.impl.groovy;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import groovy.lang.Script;

import static com.google.common.base.Preconditions.*;

public class StaticGroovyCompilationCache implements GroovyCompilationCache {

    // =================================================================================================================
    // STATIC
    // =================================================================================================================

    private static final StaticGroovyCompilationCache INSTANCE = new StaticGroovyCompilationCache();

    public static GroovyCompilationCache getInstance(){
        return INSTANCE;
    }

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final Cache<String, Class<?extends Script>> scriptCache = CacheBuilder.newBuilder().maximumSize(255).build();

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    private StaticGroovyCompilationCache(){

    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================


    @Override
    public void put(final String scriptContent, final Class<? extends Script> compiledScript) {
        checkNotNull(scriptContent, "Precondition violation - argument 'scriptContent' must not be NULL!");
        checkNotNull(compiledScript, "Precondition violation - argument 'compiledScript' must not be NULL!");
        scriptCache.put(scriptContent, compiledScript);
    }

    @Override
    public Class<? extends Script> get(final String scriptContent) {
        checkNotNull(scriptContent, "Precondition violation - argument 'scriptContent' must not be NULL!");
        return scriptCache.getIfPresent(scriptContent);
    }

    @Override
    public void clear() {
        this.scriptCache.invalidateAll();
    }
}
