package org.chronos.chronodb.test.cases.engine.indexing;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.*;
import org.chronos.chronodb.api.exceptions.ChronoDBIndexingException;
import org.chronos.chronodb.api.exceptions.UnknownIndexException;
import org.chronos.chronodb.api.indexing.DoubleIndexer;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronodb.api.indexing.LongIndexer;
import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.index.IndexManagerInternal;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.StringSearchSpecification;
import org.chronos.chronodb.internal.impl.index.IndexingOption;
import org.chronos.chronodb.internal.impl.index.SecondaryIndexImpl;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronodb.test.cases.util.model.payload.NamedPayloadNameIndexer;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.chronos.common.test.utils.NamedPayload;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class IndexingTest extends AllChronoDBBackendsTest {

    @Test
    public void indexWritingWorks() {
        ChronoDB db = this.getChronoDB();
        // set up the "name" index
        StringIndexer nameIndexer = new NamedPayloadNameIndexer();
        SecondaryIndex index = db.getIndexManager().createIndex().withName("name").withIndexer(nameIndexer).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();

        // generate and insert test data
        NamedPayload np1 = NamedPayload.create1KB("Hello World");
        NamedPayload np2 = NamedPayload.create1KB("Foo Bar");
        NamedPayload np3 = NamedPayload.create1KB("Foo Baz");
        ChronoDBTransaction tx = db.tx();
        tx.put("np1", np1);
        tx.put("np2", np2);
        tx.put("np3", np3);
        tx.commit();

        Branch masterBranch = db.getBranchManager().getMasterBranch();
        String defaultKeyspace = ChronoDBConstants.DEFAULT_KEYSPACE_NAME;

        // assert that the index is correct
        SearchSpecification<?, ?> searchSpec = StringSearchSpecification.create(
            index,
            Condition.EQUALS,
            TextMatchMode.STRICT,
            "Hello World"
        );
        Set<String> r1 = db.getIndexManager().queryIndex(
            System.currentTimeMillis(),
            masterBranch,
            defaultKeyspace,
            searchSpec
        );
        assertEquals(1, r1.size());
        assertEquals("np1", r1.iterator().next());
    }

    @Test
    public void readingNonExistentIndexFailsGracefully() {
        ChronoDB db = this.getChronoDB();
        String indexId = "dd1f3a91-12d5-47bd-a343-7b15a3475f09";
        SecondaryIndex index = new SecondaryIndexImpl(
            indexId,
            "shenaningan",
            new NamedPayloadNameIndexer(),
            Period.eternal(),
            ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            null,
            false,
            Collections.emptySet()
        );

        try {
            Branch masterBranch = db.getBranchManager().getMasterBranch();
            String defaultKeyspace = ChronoDBConstants.DEFAULT_KEYSPACE_NAME;
            SearchSpecification<?, ?> searchSpec = StringSearchSpecification.create(index, Condition.EQUALS,
                TextMatchMode.STRICT, "Hello World");
            db.getIndexManager().queryIndex(System.currentTimeMillis(), masterBranch, defaultKeyspace, searchSpec);
            fail();
        } catch (UnknownIndexException e) {
            // expected
        }
    }

    @Test
    public void renameTest() {
        ChronoDB db = this.getChronoDB();
        // set up the "name" index
        StringIndexer nameIndexer = new NamedPayloadNameIndexer();
        db.getIndexManager().createIndex().withName("name").withIndexer(nameIndexer).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();
        // generate and insert test data
        NamedPayload np1 = NamedPayload.create1KB("Hello World");
        NamedPayload np2 = NamedPayload.create1KB("Foo Bar");
        NamedPayload np3 = NamedPayload.create1KB("Foo Baz");
        ChronoDBTransaction tx = db.tx();
        tx.put("np1", np1);
        tx.put("np2", np2);
        tx.put("np3", np3);
        tx.commit();

        ChronoDBTransaction tx2 = db.tx();
        tx2.put("np1", NamedPayload.create1KB("Renamed"));
        tx2.commit();

        // check that we can find the renamed element by its new name
        assertEquals(1, db.tx().find().inDefaultKeyspace().where("name").isEqualTo("Renamed").count());
        // check that we cannot find the renamed element by its old name anymore
        assertEquals(0, db.tx().find().inDefaultKeyspace().where("name").isEqualTo("Hello World").count());
        // in the past, we should still find the non-renamed version
        assertEquals(1, tx.find().inDefaultKeyspace().where("name").isEqualTo("Hello World").count());
        // in the past, we should not find the renamed version
        assertEquals(0, tx.find().inDefaultKeyspace().where("name").isEqualTo("Renamed").count());
    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "20")
    public void deleteTest() {
        ChronoDB db = this.getChronoDB();
        // set up the "name" index
        StringIndexer nameIndexer = new NamedPayloadNameIndexer();
        db.getIndexManager().createIndex().withName("name").withIndexer(nameIndexer).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();
        // generate and insert test data
        NamedPayload np1 = NamedPayload.create1KB("np1");
        NamedPayload np2 = NamedPayload.create1KB("np2");
        NamedPayload np3 = NamedPayload.create1KB("np3");
        ChronoDBTransaction tx = db.tx();
        tx.put("np1", np1);
        tx.put("np2", np2);
        tx.put("np3", np3);
        tx.commit();

        ChronoDBTransaction tx2 = db.tx();
        assertEquals(3, db.tx().find().inDefaultKeyspace().where("name").startsWithIgnoreCase("np").count());
        tx2.remove("np1");
        tx2.remove("np3");
        tx2.commit();

        assertEquals(1, db.tx().find().inDefaultKeyspace().where("name").startsWithIgnoreCase("np").count());
        assertEquals(Collections.singleton("np2"),
            db.tx().find().inDefaultKeyspace().where("name").startsWithIgnoreCase("np").getKeysAsSet().stream()
                .map(qKey -> qKey.getKey()).collect(Collectors.toSet()));

    }

    @Test
    public void attemptingToMixIndexerTypesShouldThrowAnException() {
        ChronoDB db = this.getChronoDB();
        this.assertAddingSecondIndexerFails(db, new DummyStringIndexer(), new DummyLongIndexer());
        db.getIndexManager().clearAllIndices();
        this.assertAddingSecondIndexerFails(db, new DummyStringIndexer(), new DummyDoubleIndexer());
        db.getIndexManager().clearAllIndices();
        this.assertAddingSecondIndexerFails(db, new DummyLongIndexer(), new DummyStringIndexer());
        db.getIndexManager().clearAllIndices();
        this.assertAddingSecondIndexerFails(db, new DummyLongIndexer(), new DummyDoubleIndexer());
        db.getIndexManager().clearAllIndices();
        this.assertAddingSecondIndexerFails(db, new DummyDoubleIndexer(), new DummyStringIndexer());
        db.getIndexManager().clearAllIndices();
        this.assertAddingSecondIndexerFails(db, new DummyDoubleIndexer(), new DummyLongIndexer());
    }

    @Test
    public void attemptingToAddTheSameIndexerTwiceShouldThrowAnException() {
        ChronoDB db = this.getChronoDB();
        // this should be okay
        db.getIndexManager().createIndex().withName("name").withIndexer(new DummyFieldIndexer("name")).onMaster().acrossAllTimestamps().build();
        // ... but adding another field indexer of the same name should not be allowed
        try {
            db.getIndexManager().createIndex().withName("name").withIndexer(new DummyFieldIndexer("name")).onMaster().acrossAllTimestamps().build();
            fail("Managed to add two equal indexers to the same index!");
        } catch (ChronoDBIndexingException e) {
            // pass
        }
    }

    @Test
    public void attemptingToAddAnIndexerWithoutHashCodeAndEqualsShouldThrowAnException() {
        ChronoDB db = this.getChronoDB();
        try {
            db.getIndexManager().createIndex().withName("name").withIndexer(new IndexerWithoutHashCode()).onMaster().acrossAllTimestamps().build();
            fail("Managed to add indexer that doesn't implement hashCode()");
        } catch (ChronoDBIndexingException e) {
            // pass
        }
        try {
            db.getIndexManager().createIndex().withName("name").withIndexer(new IndexerWithoutEquals()).onMaster().acrossAllTimestamps().build();
            fail("Managed to add indexer that doesn't implement equals()");
        } catch (ChronoDBIndexingException e) {
            // pass
        }
    }

    @Test
    public void canDropAllIndices() {
        ChronoDB db = this.getChronoDB();
        db.getIndexManager().createIndex().withName("name").withIndexer(new DummyStringIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().createIndex().withName("test").withIndexer(new DummyStringIndexer()).onMaster().acrossAllTimestamps().build();
        assertEquals(2, db.getIndexManager().getIndices().size());
        db.getIndexManager().clearAllIndices();
        assertEquals(0, db.getIndexManager().getIndices().size());
    }

    @Test
    public void canDirtyAllIndices() {
        ChronoDB db = this.getChronoDB();
        db.getIndexManager().createIndex().withName("name").withIndexer(new DummyStringIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().createIndex().withName("foo").withIndexer(new DummyStringIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();
        assertEquals(Sets.newHashSet("name", "foo"), db.getIndexManager().getIndices().stream().map(SecondaryIndex::getName).collect(Collectors.toSet()));
        assertEquals(Collections.emptySet(), db.getIndexManager().getDirtyIndices());
        // dirty them all
        ((IndexManagerInternal) db.getIndexManager()).markAllIndicesAsDirty();
        assertEquals(Sets.newHashSet("name", "foo"), db.getIndexManager().getDirtyIndices().stream().map(SecondaryIndex::getName).collect(Collectors.toSet()));
        // after reindexing, they should be clean again
        db.getIndexManager().reindexAll();
        assertEquals(Collections.emptySet(), db.getIndexManager().getDirtyIndices());
    }

    @Test
    public void assumeNoPriorValuesStartsIndexClean() {
        ChronoDB db = this.getChronoDB();
        db.getIndexManager().createIndex()
            .withName("name")
            .withIndexer(new DummyStringIndexer())
            .onMaster().acrossAllTimestamps().withOption(IndexingOption.assumeNoPriorValues()).build();

        Set<SecondaryIndex> dirtyIndices = db.getIndexManager().getDirtyIndices();
        assertEquals(Collections.emptySet(), dirtyIndices);
    }

    private void assertAddingSecondIndexerFails(final ChronoDB db, final Indexer<?> indexer1, final Indexer<?> indexer2) {
        db.getIndexManager().createIndex().withName("test").withIndexer(indexer1).onMaster().acrossAllTimestamps().build();
        try {
            db.getIndexManager().createIndex().withName("test").withIndexer(indexer2).onMaster().acrossAllTimestamps().build();
            fail("Managed to mix indexer classes " + indexer1.getClass().getSimpleName() + " and " + indexer2.getClass().getName() + " in same index!");
        } catch (ChronoDBIndexingException expected) {
            // pass
        }
    }

    // =================================================================================================================
    // INNER CLASSES
    // =================================================================================================================

    private static class DummyStringIndexer implements StringIndexer {

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
            return true;
        }

        @Override
        public Set<String> getIndexValues(final Object object) {
            return Collections.singleton(String.valueOf(object));
        }
    }

    private static class DummyLongIndexer implements LongIndexer {

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
            return true;
        }

        @Override
        public Set<Long> getIndexValues(final Object object) {
            return Collections.singleton((long) String.valueOf(object).length());
        }
    }

    private static class DummyDoubleIndexer implements DoubleIndexer {

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
            return true;
        }

        @Override
        public Set<Double> getIndexValues(final Object object) {
            return Collections.singleton((double) String.valueOf(object).length());
        }
    }

    private static class DummyFieldIndexer implements StringIndexer {

        private String fieldName;

        public DummyFieldIndexer(String fieldName) {
            checkNotNull(fieldName, "Precondition violation - argument 'fieldName' must not be NULL!");
            this.fieldName = fieldName;
        }

        @Override
        public boolean canIndex(final Object object) {
            return true;
        }

        @Override
        public Set<String> getIndexValues(final Object object) {
            try {
                Field field = object.getClass().getDeclaredField(this.fieldName);
                if (field == null) {
                    return null;
                }
                field.setAccessible(true);
                Object value = field.get(object);
                if (value == null) {
                    return null;
                }
                return Collections.singleton(value.toString());
            } catch (Exception e) {
                return null;
            }
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            DummyFieldIndexer that = (DummyFieldIndexer) o;

            return Objects.equals(fieldName, that.fieldName);
        }

        @Override
        public int hashCode() {
            return fieldName != null ? fieldName.hashCode() : 0;
        }
    }

    private static class IndexerWithoutHashCode implements StringIndexer {

        // no hashCode() method here on purpose!

        @Override
        public boolean equals(Object other) {
            return other == this;
        }

        @Override
        public boolean canIndex(final Object object) {
            return true;
        }

        @Override
        public Set<String> getIndexValues(final Object object) {
            return null;
        }

    }

    private static class IndexerWithoutEquals implements StringIndexer {

        @Override
        public int hashCode() {
            return 0;
        }

        // no equals() method here on purpose!

        @Override
        public boolean canIndex(final Object object) {
            return true;
        }

        @Override
        public Set<String> getIndexValues(final Object object) {
            return null;
        }

    }
}
