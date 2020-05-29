package org.chronos.chronograph.internal.impl.transaction.trigger;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPostCommitTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPostPersistTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPreCommitTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPrePersistTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphTrigger;
import org.chronos.chronograph.api.transaction.trigger.PostCommitTriggerContext;
import org.chronos.chronograph.api.transaction.trigger.PostPersistTriggerContext;
import org.chronos.chronograph.api.transaction.trigger.PreCommitTriggerContext;
import org.chronos.chronograph.api.transaction.trigger.PrePersistTriggerContext;

import java.util.Comparator;
import java.util.Optional;

public class NamedTriggerCategoryComparator implements Comparator<Pair<String, ChronoGraphTrigger>> {


    private static final NamedTriggerCategoryComparator INSTANCE = new NamedTriggerCategoryComparator();

    public static NamedTriggerCategoryComparator getInstance() {
        return INSTANCE;
    }


    @Override
    public int compare(final Pair<String, ChronoGraphTrigger> o1, final Pair<String, ChronoGraphTrigger> o2) {
        if(o1 == null && o2 == null){
            return 0;
        }else if(o1 != null && o2 == null){
            return 1;
        }else if(o1 == null && o2 != null){
            return -1;
        }
        ChronoGraphTrigger t1 = o1.getRight();
        ChronoGraphTrigger t2 = o2.getRight();
        if(t1 instanceof ChronoGraphTriggerWrapper){
            ChronoGraphTriggerWrapper wrapper = (ChronoGraphTriggerWrapper)t1;
            t1 = wrapper.getWrappedTrigger().orElse(null);
        }
        if(t2 instanceof ChronoGraphTriggerWrapper){
            ChronoGraphTriggerWrapper wrapper = (ChronoGraphTriggerWrapper)t2;
            t2 = wrapper.getWrappedTrigger().orElse(null);
        }
        if(t1 == null && t2 == null){
            return 0;
        }else if(t1 != null && t2 == null){
            return 1;
        }else if(t1 == null && t2 != null){
            return -1;
        }
        return this.getCategoryPriority(t2) - this.getCategoryPriority(t1);
    }

    private int getCategoryPriority(ChronoGraphTrigger trigger){
        if(trigger instanceof ChronoGraphPreCommitTrigger){
            return 1000;
        }else if(trigger instanceof ChronoGraphPrePersistTrigger){
            return 750;
        }else if(trigger instanceof ChronoGraphPostPersistTrigger){
            return 500;
        } else if(trigger instanceof ChronoGraphPostCommitTrigger){
            return 250;
        }else{
            return -1;
        }
    }
}
