package org.chronos.chronograph.api.transaction.trigger;

public interface ChronoGraphPostPersistTrigger extends ChronoGraphTrigger {

    public void onPostPersist(PostPersistTriggerContext context);

}
