package org.chronos.chronodb.test.cases.engine.transaction;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.chronos.common.version.ChronosVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class ChronoDBVersionRetrievalTest extends AllChronoDBBackendsTest {

    @Test
    public void canRetrieveChronosVersionFromStrorage() {
        ChronoDB db = this.getChronoDB();
        ChronosVersion storedVersion = db.getStoredChronosVersion();
        ChronosVersion currentVersion = db.getCurrentChronosVersion();
        assertNotNull(storedVersion);
        assertNotNull(currentVersion);
        assertTrue(currentVersion.isGreaterThanOrEqualTo(storedVersion));
    }

}
