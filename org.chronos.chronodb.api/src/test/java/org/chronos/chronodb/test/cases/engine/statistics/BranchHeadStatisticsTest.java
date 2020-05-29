package org.chronos.chronodb.test.cases.engine.statistics;

import org.chronos.chronodb.api.*;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class BranchHeadStatisticsTest extends AllChronoDBBackendsTest {

    @Test
    public void canGetBranchHeadStatisticsOnEmptyDatabase(){
        ChronoDB db = this.getChronoDB();
        BranchHeadStatistics statistics = db.getStatisticsManager().getMasterBranchHeadStatistics();
        assertThat(statistics, is(notNullValue()));
        assertThat(statistics.getNumberOfEntriesInHead(), is(0L));
        assertThat(statistics.getNumberOfEntriesInHistory(), is(0L));
        assertThat(statistics.getHeadHistoryRatio(), is(1.0));
    }

    @Test
    public void hhrCalculationOnMasterIsCorrect(){
        ChronoDB db = this.getChronoDB();
        StatisticsManager statisticsManager = db.getStatisticsManager();

        ChronoDBTransaction tx = db.tx();
        tx.put("Hello", "World");
        tx.put("Foo", "Bar");
        tx.commit();

        assertThat(statisticsManager.getMasterBranchHeadStatistics().getNumberOfEntriesInHead(), is(2L));
        assertThat(statisticsManager.getMasterBranchHeadStatistics().getTotalNumberOfEntries(), is(2L));
        assertThat(statisticsManager.getMasterBranchHeadStatistics().getNumberOfEntriesInHistory(), is(0L));
        assertThat(statisticsManager.getMasterBranchHeadStatistics().getHeadHistoryRatio(), is(1.0));

        tx.put("Number", 42);
        tx.put("Foo", "Baz");
        tx.remove("Hello");
        tx.commit();

        assertThat(statisticsManager.getMasterBranchHeadStatistics().getNumberOfEntriesInHead(), is(2L));
        assertThat(statisticsManager.getMasterBranchHeadStatistics().getTotalNumberOfEntries(), is(5L));
        assertThat(statisticsManager.getMasterBranchHeadStatistics().getNumberOfEntriesInHistory(), is(3L));
        assertThat(statisticsManager.getMasterBranchHeadStatistics().getHeadHistoryRatio(), is(2.0/5.0));

    }

    @Test
    public void hhrCalculationOnBranchIsCorrect(){
        ChronoDB db = this.getChronoDB();
        StatisticsManager statisticsManager = db.getStatisticsManager();

        ChronoDBTransaction tx = db.tx();
        tx.put("Hello", "World");
        tx.put("Foo", "Bar");
        tx.commit();

        db.getBranchManager().createBranch("test");

        assertThat(statisticsManager.getBranchHeadStatistics("test"), is(notNullValue()));
        assertThat(statisticsManager.getBranchHeadStatistics("test").getTotalNumberOfEntries(), is(0L));
        assertThat(statisticsManager.getBranchHeadStatistics("test").getNumberOfEntriesInHead(), is(2L));
        assertThat(statisticsManager.getBranchHeadStatistics("test").getHeadHistoryRatio(), is(2.0));

        tx = db.tx("test");
        tx.put("Number", 42);
        tx.remove("Foo");
        tx.commit();

        assertThat(statisticsManager.getBranchHeadStatistics("test").getTotalNumberOfEntries(), is(2L));
        assertThat(statisticsManager.getBranchHeadStatistics("test").getNumberOfEntriesInHead(), is(2L));
        assertThat(statisticsManager.getBranchHeadStatistics("test").getHeadHistoryRatio(), is(1.0));
    }

    @Test
    public void hhrCalculationAfterRolloverIsCorrect(){
        ChronoDB db = this.getChronoDB();
        this.assumeRolloverIsSupported(db);

        StatisticsManager statisticsManager = db.getStatisticsManager();

        ChronoDBTransaction tx = db.tx();
        tx.put("Hello", "World");
        tx.put("Foo", "Bar");
        tx.commit();

        tx.put("Hello", "Chronos");
        tx.remove("Foo");
        tx.put("Number", 42);
        tx.commit();

        db.getMaintenanceManager().performRolloverOnMaster();

        assertThat(statisticsManager.getMasterBranchHeadStatistics().getNumberOfEntriesInHead(), is(2L));
        assertThat(statisticsManager.getMasterBranchHeadStatistics().getTotalNumberOfEntries(), is(2L));
        assertThat(statisticsManager.getMasterBranchHeadStatistics().getHeadHistoryRatio(), is(1.0));

    }


}
