package org.chronos.chronograph.internal.impl.schema;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;
import groovy.transform.CompileStatic;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronograph.api.exceptions.ChronoGraphSchemaViolationException;
import org.chronos.chronograph.api.schema.ChronoGraphSchemaManager;
import org.chronos.chronograph.api.schema.SchemaValidationResult;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.internal.impl.groovy.GroovyCompilationCache;
import org.chronos.chronograph.internal.impl.groovy.LocalGroovyCompilationCache;
import org.chronos.chronograph.internal.impl.groovy.StaticGroovyCompilationCache;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ASTTransformationCustomizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.*;

public class ChronoGraphSchemaManagerImpl implements ChronoGraphSchemaManager {

    private static final Logger log = LoggerFactory.getLogger(ChronoGraphSchemaManagerImpl.class);

    private final ChronoGraphInternal owningGraph;
    private final ReadWriteLock validatorsLock;

    private final Map<String, String> validatorScriptContentCache = Maps.newHashMap();
    private final Map<String, Script> compiledValidatorScriptCache = Maps.newHashMap();

    private final GroovyCompilationCache compilationCache;

    public ChronoGraphSchemaManagerImpl(ChronoGraphInternal owningGraph) {
        checkNotNull(owningGraph, "Precondition violation - argument 'owningGraph' must not be NULL!");
        this.owningGraph = owningGraph;
        this.validatorsLock = new ReentrantReadWriteLock(true);
        if (owningGraph.getChronoGraphConfiguration().isUseStaticGroovyCompilationCache()) {
            this.compilationCache = StaticGroovyCompilationCache.getInstance();
        } else {
            this.compilationCache = new LocalGroovyCompilationCache();
        }
        this.loadValidatorCaches();
    }

    @Override
    public boolean addOrOverrideValidator(final String validatorName, final String scriptContent) {
        return this.addOrOverrideValidator(validatorName, scriptContent, null);
    }

    @Override
    public boolean addOrOverrideValidator(final String validatorName, final String scriptContent, final Object commitMetadata) {
        checkNotNull(validatorName, "Precondition violation - argument 'validatorName' must not be NULL!");
        checkArgument(!validatorName.isEmpty(), "Precondition violation - argument 'validatorName' must not be empty!");
        checkNotNull(scriptContent, "Precondition violation - argument 'scriptContent' must not be NULL!");
        checkArgument(!scriptContent.isEmpty(), "Precondition violation - argument 'scriptContent' must not be empty!");
        Class<? extends Script> scriptClass;
        try {
            scriptClass = this.compile(scriptContent);
        } catch (Exception e) {
            throw new IllegalArgumentException("The given validator script has compilation errors. Please see root cause for details.", e);
        }
        this.validatorsLock.writeLock().lock();
        try {
            ChronoDBTransaction tx = this.owningGraph.getBackingDB().tx();
            boolean exists = tx.exists(ChronoGraphConstants.KEYSPACE_SCHEMA_VALIDATORS, validatorName);
            tx.put(ChronoGraphConstants.KEYSPACE_SCHEMA_VALIDATORS, validatorName, scriptContent);
            tx.commit(commitMetadata);
            this.addValidatorToCache(validatorName, scriptContent, scriptClass);
            return exists;
        } finally {
            this.validatorsLock.writeLock().unlock();
        }
    }

    @Override
    public boolean removeValidator(final String validatorName) {
        return this.removeValidator(validatorName, null);
    }

    @Override
    public boolean removeValidator(final String validatorName, Object commitMetadata) {
        checkNotNull(validatorName, "Precondition violation - argument 'validatorName' must not be NULL!");
        checkArgument(!validatorName.isEmpty(), "Precondition violation - argument 'validatorName' must not be empty!");
        this.validatorsLock.writeLock().lock();
        try {
            ChronoDBTransaction tx = this.owningGraph.getBackingDB().tx();
            boolean exists = tx.exists(ChronoGraphConstants.KEYSPACE_SCHEMA_VALIDATORS, validatorName);
            tx.remove(ChronoGraphConstants.KEYSPACE_SCHEMA_VALIDATORS, validatorName);
            tx.commit(commitMetadata);
            this.removeValidatorFromCache(validatorName);
            return exists;
        } finally {
            this.validatorsLock.writeLock().unlock();
        }
    }

    @Override
    public String getValidatorScript(final String validatorName) {
        checkNotNull(validatorName, "Precondition violation - argument 'validatorName' must not be NULL!");
        checkArgument(!validatorName.isEmpty(), "Precondition violation - argument 'validatorName' must not be empty!");
        this.validatorsLock.readLock().lock();
        try {
            return this.validatorScriptContentCache.get(validatorName);
        } finally {
            this.validatorsLock.readLock().unlock();
        }
    }

    @Override
    public Set<String> getAllValidatorNames() {
        this.validatorsLock.readLock().lock();
        try {
            return Collections.unmodifiableSet(Sets.newHashSet(this.validatorScriptContentCache.keySet()));
        } finally {
            this.validatorsLock.readLock().unlock();
        }
    }


    @Override
    public SchemaValidationResult validate(final String branch, final Iterable<? extends ChronoElement> elements) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkNotNull(elements, "Precondition violation - argument 'elements' must not be NULL!");
        this.validatorsLock.readLock().lock();
        try {
            SchemaValidationResultImpl result = new SchemaValidationResultImpl();
            if (this.compiledValidatorScriptCache.isEmpty()) {
                // no validators given; short-circuit the process
                return result;
            }
            for (Entry<String, Script> entry : this.compiledValidatorScriptCache.entrySet()) {
                String validatorName = entry.getKey();
                Script script = entry.getValue();
                for (ChronoElement element : elements) {
                    try {
                        this.executeValidator(script, branch, element);
                    } catch (Throwable t) {
                        if (t instanceof ChronoGraphSchemaViolationException == false) {
                            log.warn("The validator '" + validatorName + "' produced an unexpected exception. " +
                                "This will be treated as validation failure. The exception is of type '" + t.getClass().getName() +
                                "' and its message is: " + t.getMessage());
                        }
                        // record the issue
                        result.addIssue(element, validatorName, t);
                    }
                }
            }
            return result;
        } finally {
            this.validatorsLock.readLock().unlock();
        }
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private Class<? extends Script> compile(String groovyValidatorScript) throws Exception {
        checkNotNull(groovyValidatorScript, "Precondition violation - argument 'groovyValidatorScript' must not be NULL!");
        checkArgument(!groovyValidatorScript.isEmpty(), "Precondition violation - argument 'groovyValidatorScript' must not be empty!");
        Class<? extends Script> cached = this.compilationCache.get(groovyValidatorScript);
        if (cached != null) {
            return cached;
        }
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        // prepare the default import statements which we are going to use
        String imports = "import org.apache.tinkerpop.*; import org.apache.tinkerpop.gremlin.*; import org.apache.tinkerpop.gremlin.structure.*;";
        imports += "import org.chronos.chronograph.api.*; import org.chronos.chronograph.api.structure.*; import org.chronos.chronograph.api.exceptions.*;";
        // prepare the declaration of the 'element' and 'branch' variables within the script (with the proper variable type)
        String varDefs = Element.class.getSimpleName() + " element = (" + Element.class.getSimpleName() + ")this.binding.variables.element; "
            + "String branch = (String)this.binding.variables.branch;\n";
        // prepare the compiler configuration to have static type checking
        CompilerConfiguration config = new CompilerConfiguration();
        config.addCompilationCustomizers(new ASTTransformationCustomizer(CompileStatic.class));
        // compile the script into a binary class
        try (GroovyClassLoader gcl = new GroovyClassLoader(classLoader, config)) {
            Class<?extends Script> compiledScript = gcl.parseClass(imports + varDefs + groovyValidatorScript);
            this.compilationCache.put(groovyValidatorScript, compiledScript);
            return compiledScript;
        }
    }

    private void loadValidatorCaches() {
        ChronoDBTransaction tx = this.owningGraph.getBackingDB().tx();
        Set<String> validatorNames = tx.keySet(ChronoGraphConstants.KEYSPACE_SCHEMA_VALIDATORS);
        for (String validatorName : validatorNames) {
            String validatorScript = tx.get(ChronoGraphConstants.KEYSPACE_SCHEMA_VALIDATORS, validatorName);
            try {
                Class<? extends Script> compiledScript = this.compile(validatorScript);
                this.addValidatorToCache(validatorName, validatorScript, compiledScript);
            } catch (Exception e) {
                log.warn("The Graph Schema Validator '" + validatorName + "' failed to compile and will be ignored. Root cause: " + e);
            }
        }
    }

    private void addValidatorToCache(String validatorName, String validatorScript, Class<? extends Script> scriptClass) {
        checkNotNull(validatorName, "Precondition violation - argument 'validatorName' must not be NULL!");
        checkArgument(!validatorName.isEmpty(), "Precondition violation - argument 'validatorName' must not be empty!");
        checkNotNull(validatorScript, "Precondition violation - argument 'validatorScript' must not be NULL!");
        checkArgument(!validatorScript.isEmpty(), "Precondition violation - argument 'validatorScript' must not be empty!");
        checkNotNull(scriptClass, "Precondition violation - argument 'scriptClass' must not be NULL!");
        this.validatorScriptContentCache.put(validatorName, validatorScript);
        try {
            Script validatorInstance = scriptClass.getConstructor().newInstance();
            this.compiledValidatorScriptCache.put(validatorName, validatorInstance);
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
            throw new IllegalArgumentException("Could not create an instance of the given validator script. See root cause for details.", e);
        }
    }

    private void removeValidatorFromCache(String validatorName) {
        checkNotNull(validatorName, "Precondition violation - argument 'validatorName' must not be NULL!");
        checkArgument(!validatorName.isEmpty(), "Precondition violation - argument 'validatorName' must not be empty!");
        this.validatorScriptContentCache.remove(validatorName);
        this.compiledValidatorScriptCache.remove(validatorName);
    }

    private void executeValidator(Script script, String branch, ChronoElement element) throws Exception {
        Binding bindings = new Binding();
        bindings.setVariable("element", element);
        bindings.setVariable("branch", branch);
        script.setBinding(bindings);
        script.run();
    }
}
