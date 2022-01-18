package org.chronos.chronograph.internal.impl.groovy;

import groovy.lang.Script;

public interface GroovyCompilationCache {

    public void put(String scriptContent, Class<? extends Script> compiledScript);

    public Class<? extends Script> get(String scriptContent);

    public void clear();

}
