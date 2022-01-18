package org.chronos.chronograph.internal.impl.transaction.trigger;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.chronos.chronograph.api.transaction.trigger.*;
import org.chronos.common.serialization.KryoManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static com.google.common.base.Preconditions.*;

public class ChronoGraphTriggerWrapper implements ChronoGraphPreCommitTrigger, ChronoGraphPrePersistTrigger, ChronoGraphPostPersistTrigger, ChronoGraphPostCommitTrigger {

    private static final Logger log = LoggerFactory.getLogger(ChronoGraphTriggerWrapper.class);

    private byte[] serializedTrigger;
    private String triggerClassName;

    private transient ChronoGraphTrigger trigger;
    private transient boolean deserializationFailed = false;

    protected ChronoGraphTriggerWrapper(){
        // default constructor for (de-)serialization
    }

    public ChronoGraphTriggerWrapper(ChronoGraphTrigger trigger){
        checkNotNull(trigger, "Precondition violation - argument 'trigger' must not be NULL!");
        this.serializedTrigger = KryoManager.serialize(trigger);
        this.triggerClassName = trigger.getClass().getName();
        // attempt to deserialize the trigger right now
        try{
            this.trigger = KryoManager.deserialize(this.serializedTrigger);
        }catch(Exception e){
            // reject this trigger
            throw new IllegalArgumentException("The given ChronoGraphTrigger of type '" + trigger.getClass().getName() + "' does not deserialize properly." +
                " Please make sure that the class is accessible and has a no-arguments constructor. See root cause for details.", e);
        }
    }

    @Override
    public void onPostCommit(final PostCommitTriggerContext context) {
        Optional<ChronoGraphTrigger> maybeTrigger = this.getWrappedTrigger();
        if(!maybeTrigger.isPresent()){
            // trigger didn't deserialize properly
            return;
        }
        ChronoGraphTrigger trigger = maybeTrigger.get();
        if(trigger instanceof ChronoGraphPostCommitTrigger){
            ChronoGraphPostCommitTrigger postCommitTrigger = (ChronoGraphPostCommitTrigger)trigger;
            postCommitTrigger.onPostCommit(context);
        }
    }

    @Override
    public void onPostPersist(final PostPersistTriggerContext context) {
        Optional<ChronoGraphTrigger> maybeTrigger = this.getWrappedTrigger();
        if(!maybeTrigger.isPresent()){
            // trigger didn't deserialize properly
            return;
        }
        ChronoGraphTrigger trigger = maybeTrigger.get();
        if(trigger instanceof ChronoGraphPostPersistTrigger){
            ChronoGraphPostPersistTrigger postPersistTrigger = (ChronoGraphPostPersistTrigger)trigger;
            postPersistTrigger.onPostPersist(context);
        }
    }

    @Override
    public void onPreCommit(final PreCommitTriggerContext context) throws CancelCommitException {
        Optional<ChronoGraphTrigger> maybeTrigger = this.getWrappedTrigger();
        if(!maybeTrigger.isPresent()){
            // trigger didn't deserialize properly
            return;
        }
        ChronoGraphTrigger trigger = maybeTrigger.get();
        if(trigger instanceof ChronoGraphPreCommitTrigger){
            ChronoGraphPreCommitTrigger postPersistTrigger = (ChronoGraphPreCommitTrigger)trigger;
            postPersistTrigger.onPreCommit(context);
        }
    }

    @Override
    public void onPrePersist(final PrePersistTriggerContext context) throws CancelCommitException {
        Optional<ChronoGraphTrigger> maybeTrigger = this.getWrappedTrigger();
        if(!maybeTrigger.isPresent()){
            // trigger didn't deserialize properly
            return;
        }
        ChronoGraphTrigger trigger = maybeTrigger.get();
        if(trigger instanceof ChronoGraphPrePersistTrigger){
            ChronoGraphPrePersistTrigger postPersistTrigger = (ChronoGraphPrePersistTrigger)trigger;
            postPersistTrigger.onPrePersist(context);
        }
    }

    @Override
    public int getPriority() {
        return this.getWrappedTrigger().map(ChronoGraphTrigger::getPriority).orElse(0);
    }

    public String getTriggerClassName() {
        return triggerClassName;
    }

    public synchronized Optional<ChronoGraphTrigger> getWrappedTrigger(){
        // if we didn't deserialize the trigger yet, and we didn't encounter any errors in deserialization so far...
        if(this.trigger == null && !this.deserializationFailed){
            // ... try to deserialize the trigger now.
            try{
                this.trigger = KryoManager.deserialize(this.serializedTrigger);
            }catch(Exception e){
                log.warn("Failed to deserialize ChronoGraph Trigger of type [" + this.triggerClassName + "]. This trigger will be ignored!" +
                    " Cause: " + e + ", Root Cause: " + ExceptionUtils.getRootCause(e)
                );
                // remember that this trigger cannot be deserialized
                this.deserializationFailed = true;
            }
        }
        // at this point, the trigger may or may not be NULL.
        return Optional.ofNullable(this.trigger);
    }
}
