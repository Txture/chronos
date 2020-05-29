package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.api.transaction.ChronoGraphTransactionManager;

import java.util.Date;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.*;

public class ReadOnlyChronoGraphTransactionManager implements ChronoGraphTransactionManager {

    private ChronoGraphTransactionManager manager;

    public ReadOnlyChronoGraphTransactionManager(ChronoGraphTransactionManager manager){
        checkNotNull(manager, "Precondition violation - argument 'manager' must not be NULL!");
        this.manager = manager;
    }


    @Override
    public void open(final long timestamp) {
        this.unsupportedOperation();
    }

    @Override
    public void open(final Date date) {
        this.unsupportedOperation();
    }

    @Override
    public void open(final String branch) {
        this.unsupportedOperation();
    }

    @Override
    public void open(final String branch, final long timestamp) {
        this.unsupportedOperation();
    }

    @Override
    public void open(final String branch, final Date date) {
        this.unsupportedOperation();
    }

    @Override
    public void reset() {
        this.unsupportedOperation();
    }

    @Override
    public void reset(final long timestamp) {
        this.unsupportedOperation();
    }

    @Override
    public void reset(final Date date) {
        this.unsupportedOperation();
    }

    @Override
    public void reset(final String branch, final long timestamp) {
        this.unsupportedOperation();
    }

    @Override
    public void reset(final String branch, final Date date) {
        this.unsupportedOperation();
    }

    @Override
    public ChronoGraph createThreadedTx() {
        return this.unsupportedOperation();
    }

    @Override
    public ChronoGraph createThreadedTx(final long timestamp) {
        return this.unsupportedOperation();
    }

    @Override
    public ChronoGraph createThreadedTx(final Date date) {
        return this.unsupportedOperation();
    }

    @Override
    public ChronoGraph createThreadedTx(final String branchName) {
        return this.unsupportedOperation();
    }

    @Override
    public ChronoGraph createThreadedTx(final String branchName, final long timestamp) {
        return this.unsupportedOperation();
    }

    @Override
    public ChronoGraph createThreadedTx(final String branchName, final Date date) {
        return this.unsupportedOperation();
    }

    @Override
    public void commitIncremental() {
        this.unsupportedOperation();
    }

    @Override
    public void commit(final Object metadata) {
        this.unsupportedOperation();
    }

    @Override
    public long commitAndReturnTimestamp() {
        return this.unsupportedOperation();
    }

    @Override
    public long commitAndReturnTimestamp(final Object metadata) {
        return this.unsupportedOperation();
    }

    @Override
    public ChronoGraphTransaction getCurrentTransaction() {
        return new ReadOnlyChronoGraphTransaction(this.manager.getCurrentTransaction());
    }

    @Override
    public void open() {
        this.unsupportedOperation();
    }

    @Override
    public void commit() {
        this.unsupportedOperation();
    }

    @Override
    public void rollback() {
        // transaction was read-only to begin with; nothing to do!
    }

    @Override
    public boolean isOpen() {
        return this.manager.isOpen();
    }

    @Override
    public void readWrite() {
        this.unsupportedOperation();
    }

    @Override
    public void close() {
        this.unsupportedOperation();
    }

    @Override
    public Transaction onReadWrite(final Consumer<Transaction> consumer) {
        return this.unsupportedOperation();
    }

    @Override
    public Transaction onClose(final Consumer<Transaction> consumer) {
        return this.unsupportedOperation();
    }

    @Override
    public void addTransactionListener(final Consumer<Status> listener) {
        this.unsupportedOperation();
    }

    @Override
    public void removeTransactionListener(final Consumer<Status> listener) {
        this.unsupportedOperation();
    }

    @Override
    public void clearTransactionListeners() {
        this.unsupportedOperation();
    }

    private <T> T unsupportedOperation(){
        throw new UnsupportedOperationException("This operation is not supported on a read-only graph!");
    }
}
