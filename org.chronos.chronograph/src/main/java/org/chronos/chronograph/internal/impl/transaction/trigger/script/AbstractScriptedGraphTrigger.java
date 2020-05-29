package org.chronos.chronograph.internal.impl.transaction.trigger.script;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;
import groovy.transform.CompileStatic;
import org.chronos.chronograph.api.exceptions.GraphTriggerScriptCompilationException;
import org.chronos.chronograph.api.exceptions.GraphTriggerScriptInstantiationException;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphTrigger;
import org.chronos.chronograph.api.transaction.trigger.TriggerContext;
import org.chronos.common.annotation.PersistentClass;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ASTTransformationCustomizer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.*;

@PersistentClass("kryo")
public abstract class AbstractScriptedGraphTrigger implements ChronoGraphTrigger {

    // =================================================================================================================
    // STATIC PART
    // =================================================================================================================

    private static transient LoadingCache<String, Class<? extends Script>> COMPILED_TRIGGERS_CODE_CACHE =
        CacheBuilder.newBuilder()
            .maximumSize(1000)
            .build(CacheLoader.from(AbstractScriptedGraphTrigger::runGroovyCompile));

    @SuppressWarnings("unchecked")
    private static Class<? extends Script> runGroovyCompile(String scriptContent) {
        System.out.println(scriptContent);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        // prepare the compiler configuration to have static type checking
        CompilerConfiguration config = new CompilerConfiguration();
        config.addCompilationCustomizers(new ASTTransformationCustomizer(CompileStatic.class));
        try (GroovyClassLoader gcl = new GroovyClassLoader(classLoader, config)) {
            return (Class<? extends Script>) gcl.parseClass(scriptContent);
        } catch (IOException e) {
            throw new GraphTriggerScriptCompilationException("Failed to compile Groovy Script for GraphScriptTrigger. See root cause for details.", e);
        }
    }

    // =================================================================================================================
    // SERIALIZED FIELDS
    // =================================================================================================================

    private String userScriptContent;
    private int priority;

    // =================================================================================================================
    // CACHES
    // =================================================================================================================

    private transient Class<? extends Script> compiledScriptClass;
    private transient Script scriptInstance;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected AbstractScriptedGraphTrigger(String userScriptContent, int priority) {
        checkNotNull(userScriptContent, "Precondition violation - argument 'userScriptContent' must not be NULL!");
        this.userScriptContent = userScriptContent;
        this.priority = priority;
        // make sure that the script actually compiles
        this.getCompiledScriptInstance();
    }

    protected AbstractScriptedGraphTrigger() {
        // default constructor for (de-)serialization.
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public int getPriority() {
        return this.priority;
    }

    public String getUserScriptContent() {
        return this.userScriptContent;
    }

    @SuppressWarnings("unchecked")
    public synchronized Class<? extends Script> getCompiledScriptClass() {
        if (this.compiledScriptClass != null) {
            return this.compiledScriptClass;
        }
        if (this.userScriptContent == null) {
            throw new IllegalStateException("Graph Script Trigger: User Script Content is NULL!");
        }
        // prepare the default import statements which we are going to use
        String imports = "import org.apache.tinkerpop.*; import org.apache.tinkerpop.gremlin.*; import org.apache.tinkerpop.gremlin.structure.*;";
        imports += "import org.chronos.chronograph.api.*; import org.chronos.chronograph.api.structure.*; import org.chronos.chronograph.api.exceptions.*;";
        imports += "import org.chronos.chronograph.api.transaction.*;import org.chronos.chronograph.api.transaction.trigger.*;";
        // prepare the declaration of the 'context' variable within the script (with the proper variable type)
        String varDefs = this.getTriggerContextClass().getName() + " context = (" + this.getTriggerContextClass().getName() + ")this.binding.variables.context;\n";
        // prepare the compiler configuration to have static type checking
        CompilerConfiguration config = new CompilerConfiguration();
        config.addCompilationCustomizers(new ASTTransformationCustomizer(CompileStatic.class));
        // compile the script into a binary class
        String triggerScript = imports + varDefs + this.userScriptContent;
        try{
            return COMPILED_TRIGGERS_CODE_CACHE.get(triggerScript);
        }catch(ExecutionException e){
            throw new GraphTriggerScriptCompilationException("Failed to compile Groovy Script for GraphScriptTrigger. See root cause for details.", e);
        }
    }

    public synchronized Script getCompiledScriptInstance() {
        if (this.scriptInstance != null) {
            return this.scriptInstance;
        }
        Class<? extends Script> scriptClass = this.getCompiledScriptClass();
        try {
            this.scriptInstance = scriptClass.getConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new GraphTriggerScriptInstantiationException("Failed to instantiate Graph Trigger Script. See root cause for details.", e);
        }
        return this.scriptInstance;
    }

    protected abstract Class<? extends TriggerContext> getTriggerContextClass();

}
