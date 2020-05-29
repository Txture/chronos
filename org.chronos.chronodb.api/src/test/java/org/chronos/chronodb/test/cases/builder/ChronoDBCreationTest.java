package org.chronos.chronodb.test.cases.builder;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class ChronoDBCreationTest extends AllChronoDBBackendsTest {

    @Test
    public void dbCreationWorks() {
        ChronoDB chronoDB = this.getChronoDB();
        assertNotNull("Failed to instantiate ChronoDB on Backend '" + this.getChronoBackendName() + "'!", chronoDB);
    }

}
