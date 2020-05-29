package org.chronos.chronodb.test.cases.engine.transaction;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.CommitMetadataFilter;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitMetadataRejectedException;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.junit.Test;

import static org.junit.Assert.*;

public class CommitMetadataFilterTest extends AllChronoDBBackendsTest {

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.COMMIT_METADATA_FILTER_CLASS, value = "org.chronos.chronodb.test.cases.engine.transaction.CommitMetadataFilterTest$MyTestFilter")
    public void canAttachCommitMetadataFilter() {
        ChronoDB db = this.getChronoDB();
        try {
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "World");
            // commit without argument commits a NULL metadata object
            tx.commit();
            fail("Failed to commit NULL metadata object through filter that does not permit NULL values!");
        } catch (ChronoDBCommitMetadataRejectedException expected) {
            // pass
        }
        ChronoDBTransaction tx = db.tx();
        tx.put("Hello", "World");
        // this should work (because the metadata is not null)
        tx.commit("I did this!");
    }

    private static class MyTestFilter implements CommitMetadataFilter {

        public MyTestFilter() {
            // default constructor for instantiation
        }

        @Override
        public boolean doesAccept(final String branch, final long timestamp, final Object metadata) {
            // do not permit NULL values
            return metadata != null;
        }
    }
}
