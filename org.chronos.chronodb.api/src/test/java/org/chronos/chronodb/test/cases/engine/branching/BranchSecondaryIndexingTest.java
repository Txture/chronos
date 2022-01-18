package org.chronos.chronodb.test.cases.engine.branching;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.exceptions.ChronoDBIndexingException;
import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronodb.test.cases.util.model.person.FirstNameIndexer;
import org.chronos.chronodb.test.cases.util.model.person.LastNameIndexer;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.chronos.common.test.utils.model.person.Person;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class BranchSecondaryIndexingTest extends AllChronoDBBackendsTest {

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "1000")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "20")
    public void canAddMultiplicityManyValuesInBranch() {
        ChronoDB db = this.getChronoDB();
        db.getIndexManager().createIndex().withName("name").withIndexer(new TestObjectNameIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();

        ChronoDBTransaction tx1 = db.tx();
        tx1.put("one", new TestObject("One", "TO_One"));
        tx1.put("two", new TestObject("Two", "TO_Two"));
        tx1.commit();

        // assert that we can now find these objects
        assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("One").count());
        assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("TO_One").count());
        assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("Two").count());
        assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("TO_Two").count());

        // create a branch
        Branch branch = db.getBranchManager().createBranch("MyBranch");
        assertNotNull(branch);
        assertEquals(db.getBranchManager().getMasterBranch(), branch.getOrigin());
        assertEquals(tx1.getTimestamp(), branch.getBranchingTimestamp());

        // in the branch, add an additional value to "Two"
        ChronoDBTransaction tx2 = db.tx("MyBranch");
        tx2.put("two", new TestObject("Two", "TO_Two", "Hello World"));
        tx2.commit();

        // now, in the branch, we should be able to find the new entry with a query
        assertEquals(1, db.tx("MyBranch").find().inDefaultKeyspace().where("name").containsIgnoreCase("world").count());
        // we still should find the "one" entry
        assertEquals(1, db.tx("MyBranch").find().inDefaultKeyspace().where("name").containsIgnoreCase("one").count());

        // in the master branch, we know nothing about it
        assertEquals(0, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("world").count());
    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "1000")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "20")
    public void canChangeMultiplicityManyValuesInBranch() {
        ChronoDB db = this.getChronoDB();
        db.getIndexManager().createIndex().withName("name").withIndexer(new TestObjectNameIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();

        ChronoDBTransaction tx1 = db.tx();
        tx1.put("one", new TestObject("One", "TO_One"));
        tx1.put("two", new TestObject("Two", "TO_Two"));
        tx1.commit();

        // assert that we can now find these objects
        assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("One").count());
        assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("TO_One").count());
        assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("Two").count());
        assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("TO_Two").count());

        // create a branch
        Branch branch = db.getBranchManager().createBranch("MyBranch");
        assertNotNull(branch);
        assertEquals(db.getBranchManager().getMasterBranch(), branch.getOrigin());
        assertEquals(tx1.getTimestamp(), branch.getBranchingTimestamp());

        // in the branch, add an additional value to "Two"
        ChronoDBTransaction tx2 = db.tx("MyBranch");
        tx2.put("two", new TestObject("Two", "Hello World"));
        tx2.commit();

        // now, in the branch, we should be able to find the new entry with a query
        assertEquals(1, db.tx("MyBranch").find().inDefaultKeyspace().where("name").containsIgnoreCase("world").count());
        // we shouldn't be able to find it under "TO_Two" in the branch
        assertEquals(0, db.tx("MyBranch").find().inDefaultKeyspace().where("name").isEqualTo("TO_Two").count());

        // in the master branch, we know nothing about the change
        assertEquals(0, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("world").count());
        // ... but we still find the object under its previous name
        assertEquals(1, db.tx().find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("Two").count());
    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "1000")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "20")
    public void canAddNewIndexedValuesInBranch() {
        ChronoDB db = this.getChronoDB();
        db.getIndexManager().createIndex().withName("name").withIndexer(new TestObjectNameIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();

        ChronoDBTransaction tx1 = db.tx();
        tx1.put("one", new TestObject("One"));
        tx1.commit();

        // assert that we can now find the objects
        assertEquals(1, tx1.find().inDefaultKeyspace().where("name").isEqualToIgnoreCase("One").count());

        // create a branch
        Branch branch = db.getBranchManager().createBranch("MyBranch");
        assertNotNull(branch);
        assertEquals(db.getBranchManager().getMasterBranch(), branch.getOrigin());
        assertEquals(tx1.getTimestamp(), branch.getBranchingTimestamp());

        // in the branch, add an additional value to "Two"
        ChronoDBTransaction tx2 = db.tx("MyBranch");
        tx2.put("two", new TestObject("Two"));
        tx2.commit();

        // now, in the branch, we should be able to find the new entry with a query
        assertEquals(1, db.tx("MyBranch").find().inDefaultKeyspace().where("name").containsIgnoreCase("two").count());

        // in the master branch, we know nothing about it
        assertEquals(0, db.tx().find().inDefaultKeyspace().where("name").containsIgnoreCase("two").count());
    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "1000")
    public void canAddSecondaryIndexOnBranch() {
        ChronoDB db = this.getChronoDB();

        ChronoDBTransaction tx1 = db.tx();
        tx1.put("p1", new Person("John", "Doe"));
        tx1.put("p2", new Person("Jane", "Doe"));
        tx1.commit();

        db.getBranchManager().createBranch("myBranch");

        ChronoDBTransaction tx2 = db.tx("myBranch");
        tx2.put("p3", new Person("Jack", "Smith"));
        long commit2 = tx2.commit();

        db.getIndexManager().createIndex()
            .withName("firstName")
            .withIndexer(new FirstNameIndexer())
            .onBranch("myBranch")
            .fromTimestamp(commit2)
            .toInfinity()
            .build();

        db.getIndexManager().reindexAll();

        Set<QualifiedKey> queryResult1 = db.tx("myBranch").find()
            .inDefaultKeyspace()
            .where("firstName")
            .startsWith("Ja")
            .getKeysAsSet();
        assertEquals(
            Sets.newHashSet(
                QualifiedKey.createInDefaultKeyspace("p2"), // Jane
                QualifiedKey.createInDefaultKeyspace("p3") // Jack
            ),
            queryResult1
        );

        ChronoDBTransaction tx3 = db.tx("myBranch");
        tx3.put("p4", new Person("Janine", "Smith"));
        tx3.commit();

        Set<QualifiedKey> queryResult2 = db.tx("myBranch").find()
            .inDefaultKeyspace()
            .where("firstName")
            .startsWith("Ja")
            .getKeysAsSet();
        assertEquals(
            Sets.newHashSet(
                QualifiedKey.createInDefaultKeyspace("p2"), // Jane
                QualifiedKey.createInDefaultKeyspace("p3"), // Jack
                QualifiedKey.createInDefaultKeyspace("p4") // Janine
            ),
            queryResult2
        );
    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "1000")
    public void canAddTimeLimitedIndicesAfterwards() {
        ChronoDB db = this.getChronoDB();

        ChronoDBTransaction tx1 = db.tx();
        tx1.put("p1", new Person("John", "Doe"));
        tx1.put("p2", new Person("Jane", "Doe"));
        long commit1 = tx1.commit();

        sleep(5);

        ChronoDBTransaction tx2 = db.tx();
        tx2.put("p3", new Person("Jack", "Smith"));
        long commit2 = tx2.commit();

        sleep(5);

        if (db.getFeatures().isRolloverSupported()) {
            db.getMaintenanceManager().performRolloverOnMaster();
        }

        sleep(5);

        ChronoDBTransaction tx3 = db.tx();
        tx3.put("p4", new Person("Janine", "Smith"));
        long commit3 = tx3.commit();

        sleep(5);

        ChronoDBTransaction tx4 = db.tx();
        tx4.remove("p1");
        long commit4 = tx4.commit();

        sleep(5);

        if (db.getFeatures().isRolloverSupported()) {
            db.getMaintenanceManager().performRolloverOnMaster();
        }

        sleep(5);

        ChronoDBTransaction tx5 = db.tx();
        tx5.put("p5", new Person("Sarah", "Doe"));
        long commit5 = tx5.commit();

        System.out.println("Commit1:" + commit1);
        System.out.println("Commit2:" + commit2);
        System.out.println("Commit3:" + commit3);
        System.out.println("Commit4:" + commit4);
        System.out.println("Commit5:" + commit5);

        db.getIndexManager().createIndex()
            .withName("firstName")
            .withIndexer(new FirstNameIndexer())
            .onMaster()
            .fromTimestamp(0).toTimestamp(commit2 + 2)
            .build();

        db.getIndexManager().createIndex()
            .withName("firstName")
            .withIndexer(new FirstNameIndexer())
            .onMaster()
            .fromTimestamp(commit4)
            .toInfinity()
            .build();
        db.getIndexManager().createIndex().withName("lastName").withIndexer(new LastNameIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();

        // Check index "firstName" which exists in periods
        assertEquals(0, db.tx(commit1 - 1).find().inDefaultKeyspace().where("firstName").startsWith("J").count());
        assertEquals(2, db.tx(commit1).find().inDefaultKeyspace().where("firstName").startsWith("J").count());
        assertEquals(2, db.tx(commit1 + 1).find().inDefaultKeyspace().where("firstName").startsWith("J").count());
        assertEquals(2, db.tx(commit2 - 1).find().inDefaultKeyspace().where("firstName").startsWith("J").count());
        assertEquals(3, db.tx(commit2).find().inDefaultKeyspace().where("firstName").startsWith("J").count());
        assertEquals(3, db.tx(commit2 + 1).find().inDefaultKeyspace().where("firstName").startsWith("J").count());
        // unavailable on commit3
        assertEquals(Sets.newHashSet(
                "p3", "p2", "p4"
            )
            , db.tx(commit4).find().inDefaultKeyspace().where("firstName").startsWith("J").getKeysAsSet().stream().map(QualifiedKey::getKey).collect(Collectors.toSet()));
        assertEquals(3, db.tx(commit4).find().inDefaultKeyspace().where("firstName").startsWith("J").count());
        assertEquals(3, db.tx(commit4 + 1).find().inDefaultKeyspace().where("firstName").startsWith("J").count());
        assertEquals(3, db.tx(commit5 - 1).find().inDefaultKeyspace().where("firstName").startsWith("J").count());
        assertEquals(3, db.tx(commit5).find().inDefaultKeyspace().where("firstName").startsWith("J").count());

        // Check index "lastName" which exists on all timestamps
        assertEquals(0, db.tx(commit1 - 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit1 + 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit2 - 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit2).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit2 + 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit3 - 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit3).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit3 + 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit4 - 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(1, db.tx(commit4).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(1, db.tx(commit4 + 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(1, db.tx(commit5 - 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit5).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "1000")
    public void canAddIndexOnHead() {
        ChronoDB db = this.getChronoDB();

        db.getIndexManager().createIndex().withName("lastName").withIndexer(new LastNameIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();

        ChronoDBTransaction tx1 = db.tx();
        tx1.put("p1", new Person("John", "Doe"));
        tx1.put("p2", new Person("Jane", "Doe"));
        long commit1 = tx1.commit();

        sleep(5);

        ChronoDBTransaction tx2 = db.tx();
        tx2.put("p3", new Person("Jack", "Smith"));
        long commit2 = tx2.commit();

        sleep(5);

        if (db.getFeatures().isRolloverSupported()) {
            db.getMaintenanceManager().performRolloverOnMaster();
        }

        sleep(5);

        ChronoDBTransaction tx3 = db.tx();
        tx3.put("p4", new Person("Janine", "Smith"));
        long commit3 = tx3.commit();

        sleep(5);

        db.getIndexManager().createIndex()
            .withName("firstName")
            .withIndexer(new FirstNameIndexer())
            .onMaster()
            .fromTimestamp(db.tx().getTimestamp())
            .toInfinity()
            .build();
        db.getIndexManager().reindexAll();

        ChronoDBTransaction tx4 = db.tx();
        tx4.remove("p1");
        long commit4 = tx4.commit();

        sleep(5);

        if (db.getFeatures().isRolloverSupported()) {
            db.getMaintenanceManager().performRolloverOnMaster();
        }

        sleep(5);

        ChronoDBTransaction tx5 = db.tx();
        tx5.put("p5", new Person("Sarah", "Doe"));
        long commit5 = tx5.commit();

        System.out.println("Commit1:" + commit1);
        System.out.println("Commit2:" + commit2);
        System.out.println("Commit3:" + commit3);
        System.out.println("Commit4:" + commit4);
        System.out.println("Commit5:" + commit5);

        try {
            db.tx(commit3 - 1).find().inDefaultKeyspace().where("firstName").startsWith("J").count();
            fail("Managed to query index outside its validity range");
        } catch (ChronoDBIndexingException expected) {
            // pass
        }

        assertEquals(
            Sets.newHashSet("p3", "p2", "p4"),
            db.tx(commit4).find().inDefaultKeyspace().where("firstName").startsWith("J").getUnqualifiedKeysAsSet()
        );
        assertEquals(3, db.tx(commit4).find().inDefaultKeyspace().where("firstName").startsWith("J").count());
        assertEquals(3, db.tx(commit4 + 1).find().inDefaultKeyspace().where("firstName").startsWith("J").count());
        assertEquals(3, db.tx(commit5 - 1).find().inDefaultKeyspace().where("firstName").startsWith("J").count());
        assertEquals(3, db.tx(commit5).find().inDefaultKeyspace().where("firstName").startsWith("J").count());

        // Check index "lastName" which exists on all timestamps
        assertEquals(0, db.tx(commit1 - 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit1 + 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit2 - 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit2).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit2 + 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit3 - 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit3).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit3 + 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit4 - 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(1, db.tx(commit4).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(1, db.tx(commit4 + 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(1, db.tx(commit5 - 1).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
        assertEquals(2, db.tx(commit5).find().inDefaultKeyspace().where("lastName").isEqualTo("Doe").count());
    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "1000")
    public void canAddIndexOnDeltaChunk() {
        ChronoDB db = this.getChronoDB();

        ChronoDBTransaction tx1 = db.tx();
        tx1.put("p1", new Person("John", "Doe"));
        tx1.put("p2", new Person("Jane", "Doe"));
        tx1.put("p3", new Person("Jack", "Smith"));
        tx1.put("p4", new Person("Janine", "Smith"));
        tx1.commit();

        ChronoDBTransaction tx2 = db.tx();
        tx2.remove("p1");
        tx2.commit();

        db.getBranchManager().createBranch("myBranch");

        ChronoDBTransaction tx3 = db.tx();
        tx3.remove("p4");
        tx3.put("p5", new Person("Sarah", "Doe"));
        tx3.commit();

        ChronoDBTransaction tx4 = db.tx("myBranch");
        tx4.remove("p3");
        tx4.put("p2", new Person("NoLongerJane", "Doe"));
        tx4.put("p6", new Person("Jeanne", "Brancher"));
        long commit4 = tx4.commit();

        db.getIndexManager().createIndex()
            .withName("firstName")
            .withIndexer(new FirstNameIndexer())
            .onBranch("myBranch")
            .fromTimestamp(commit4)
            .toInfinity()
            .build();

        db.getIndexManager().reindexAll();

        Set<String> queryResult = db.tx("myBranch").find()
            .inDefaultKeyspace()
            .where("firstName")
            .startsWith("J")
            .getUnqualifiedKeysAsSet();

        assertEquals(Sets.newHashSet("p4", "p6"), queryResult);
    }

    private static class TestObject {

        private Set<String> names;

        @SuppressWarnings("unused")
        protected TestObject() {
            // for serialization
        }

        public TestObject(final String... names) {
            this.names = Sets.newHashSet(names);
        }

        public Set<String> getNames() {
            return this.names;
        }

    }

    private static class TestObjectNameIndexer implements StringIndexer {

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            return super.equals(obj);
        }

        @Override
        public boolean canIndex(final Object object) {
            return object instanceof TestObject;
        }

        @Override
        public Set<String> getIndexValues(final Object object) {
            TestObject testObject = (TestObject) object;
            return Sets.newHashSet(testObject.getNames());
        }

    }
}
