package org.chronos.chronodb.test.cases.settings;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.exceptions.TransactionIsReadOnlyException;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class ChronoDBConfigurationTest extends AllChronoDBBackendsTest {

    @Test
    public void debugModeIsAlwaysEnabledInTests() {
        ChronoDB db = this.getChronoDB();
        assertTrue(db.getConfiguration().isDebugModeEnabled());
    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.READONLY, value = "true")
    public void canOpenDatabaseInReadOnlyMode() {
        ChronoDB db = this.getChronoDB();
        assertTrue(db.getConfiguration().isReadOnly());
        // this shouldn't throw any exceptions...
        assertNull(db.tx().get("hello"));
        // but this should
        try {
            ChronoDBTransaction tx = db.tx();
            tx.put("foo", "bar");
            tx.commit();
            fail("Managed to write to a read-only chronodb!");
        } catch (IllegalStateException | TransactionIsReadOnlyException expected) {
            // pass
        }
    }
}
