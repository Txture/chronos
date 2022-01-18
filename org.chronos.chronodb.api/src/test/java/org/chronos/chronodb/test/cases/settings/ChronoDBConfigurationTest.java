package org.chronos.chronodb.test.cases.settings;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.exceptions.TransactionIsReadOnlyException;
import org.chronos.chronodb.exodus.configuration.ExodusChronoDBConfiguration;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class ChronoDBConfigurationTest extends AllChronoDBBackendsTest {

    @Test
    public void debugModeIsAlwaysEnabledInTests() {
        ChronoDB db = this.getChronoDB();
        assertTrue(db.getConfiguration().isDebugModeEnabled());
    }

}
