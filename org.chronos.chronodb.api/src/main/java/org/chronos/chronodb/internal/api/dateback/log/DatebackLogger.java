package org.chronos.chronodb.internal.api.dateback.log;

@FunctionalInterface
public interface DatebackLogger {

    public void logDatebackOperation(DatebackOperation operation);

}
