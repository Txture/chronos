package org.chronos.chronograph.internal.impl.transaction.trigger.script;

import groovy.lang.Binding;
import groovy.lang.Script;
import org.apache.commons.lang3.StringUtils;
import org.chronos.chronograph.api.transaction.trigger.CancelCommitException;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPrePersistTrigger;
import org.chronos.chronograph.api.transaction.trigger.PrePersistTriggerContext;
import org.chronos.chronograph.api.transaction.trigger.TriggerContext;
import org.chronos.common.annotation.PersistentClass;

import java.util.Arrays;
import java.util.stream.Collectors;

@PersistentClass("kryo")
public class ScriptedGraphPrePersistTrigger extends AbstractScriptedGraphTrigger implements ChronoGraphPrePersistTrigger {

    public ScriptedGraphPrePersistTrigger(String userScriptContent, int priority){
        super(userScriptContent, priority);
    }

    protected ScriptedGraphPrePersistTrigger(){
        // default constructor for (de-)serialization.
    }

    @Override
    public void onPrePersist(final PrePersistTriggerContext context) throws CancelCommitException {
        Binding binding = new Binding();
        binding.setVariable("context", context);
        Script userScript = this.getCompiledScriptInstance();
        userScript.setBinding(binding);
        userScript.run();
    }

    @Override
    protected Class<? extends TriggerContext> getTriggerContextClass() {
        return PrePersistTriggerContext.class;
    }

    @Override
    public String toString() {
        String[] lines = this.getUserScriptContent().split("\\r?\\n");
        String userScriptDigest = StringUtils.abbreviate(Arrays.stream(lines).skip(1).collect(Collectors.joining(" ", "", "")), 0, 100);
        return "PrePersistGraphScriptTrigger[" + userScriptDigest  + "]";
    }
}
