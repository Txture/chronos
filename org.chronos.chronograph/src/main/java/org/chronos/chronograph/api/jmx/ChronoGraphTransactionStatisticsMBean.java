package org.chronos.chronograph.api.jmx;

public interface ChronoGraphTransactionStatisticsMBean {

    public long getNumberOfVertexRecordRefetches();

    public void incrementNumberOfVertexRecordRefetches();

    public void resetNumberOfVertexRecordRefetches();

}
