package org.chronos.chronograph.internal.impl.transaction.trigger;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.shaded.jackson.databind.util.Named;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphTrigger;

import java.util.Comparator;

public class NamedTriggerComparator implements Comparator<Pair<String, ChronoGraphTrigger>> {

    private static final NamedTriggerComparator INSTANCE = new NamedTriggerComparator();

    public static NamedTriggerComparator getInstance() {
        return INSTANCE;
    }

    @Override
    public int compare(final Pair<String, ChronoGraphTrigger> o1, final Pair<String, ChronoGraphTrigger> o2) {
        if(o1 == null && o2 == null){
            return 0;
        }
        if(o1 == null && o2 != null){
            return -1;
        }
        if(o1 != null && o2 == null){
            return 1;
        }
        int prioLeft = o1.getRight().getPriority();
        int prioRight = o2.getRight().getPriority();
        if(prioLeft == prioRight){
            return 0;
        }else if(prioLeft < prioRight){
            return -1;
        }else{
            return 1;
        }
    }
}
