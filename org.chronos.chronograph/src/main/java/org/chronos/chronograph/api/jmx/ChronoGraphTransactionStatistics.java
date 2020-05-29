package org.chronos.chronograph.api.jmx;

import java.util.concurrent.atomic.AtomicLong;

public class ChronoGraphTransactionStatistics implements ChronoGraphTransactionStatisticsMBean {

    private static final ChronoGraphTransactionStatisticsMBean INSTANCE = new ChronoGraphTransactionStatistics();

    public static ChronoGraphTransactionStatisticsMBean getInstance(){
        return INSTANCE;
    }

    private AtomicLong numberOfVertexRecordRefetches = new AtomicLong(0);

    @Override
    public long getNumberOfVertexRecordRefetches() {
        return this.numberOfVertexRecordRefetches.get();
    }

    @Override
    public void incrementNumberOfVertexRecordRefetches() {
        this.numberOfVertexRecordRefetches.incrementAndGet();
    }

    @Override
    public void resetNumberOfVertexRecordRefetches() {
        this.numberOfVertexRecordRefetches.set(0);
    }

}
