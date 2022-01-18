package org.chronos.chronodb.test.cases.engine.indexing.diff;

import com.google.common.collect.Sets;
import org.chronos.chronodb.internal.impl.index.diff.MutableIndexValueDiff;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.chronodb.test.cases.util.model.person.FirstNameIndexer;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class MutableIndexValueDiffTest extends ChronoDBUnitTest {

    @Test
    public void canCreateEmptyDiff() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(null, null);
        assertNotNull(diff);
        assertTrue(diff.isEmpty());
        assertFalse(diff.isEntryAddition());
        assertFalse(diff.isEntryRemoval());
        assertFalse(diff.isEntryUpdate());
    }

    @Test
    public void canCreateEmptyAddition() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(null, new Object());
        assertNotNull(diff);
        assertTrue(diff.isEmpty());
        assertTrue(diff.isEntryAddition());
        assertFalse(diff.isEntryRemoval());
        assertFalse(diff.isEntryUpdate());
    }

    @Test
    public void canCreateEmptyRemoval() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(new Object(), null);
        assertNotNull(diff);
        assertTrue(diff.isEmpty());
        assertFalse(diff.isEntryAddition());
        assertTrue(diff.isEntryRemoval());
        assertFalse(diff.isEntryUpdate());
    }

    @Test
    public void canCreateEmtpyUpdate() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(new Object(), new Object());
        assertNotNull(diff);
        assertTrue(diff.isEmpty());
        assertFalse(diff.isEntryAddition());
        assertFalse(diff.isEntryRemoval());
        assertTrue(diff.isEntryUpdate());
    }

    @Test
    public void canInsertAdditions() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(new Object(), new Object());
        assertTrue(diff.isEmpty());
        assertFalse(diff.isEntryAddition());
        assertFalse(diff.isEntryRemoval());
        assertTrue(diff.isEntryUpdate());
        DummyIndex testIndex = new DummyIndex(new FirstNameIndexer(), "test");
        DummyIndex testIndex2 = new DummyIndex(new FirstNameIndexer(), "test2");
        DummyIndex testIndex3 = new DummyIndex(new FirstNameIndexer(), "test3");
        diff.add(testIndex, "Hello World");
        diff.add(testIndex, "Foo Bar");
        diff.add(testIndex2, "Baz");
        assertEquals(Sets.newHashSet(testIndex, testIndex2), diff.getChangedIndices());
        assertEquals(Sets.newHashSet("Hello World", "Foo Bar"), diff.getAdditions(testIndex));
        assertEquals(Sets.newHashSet("Baz"), diff.getAdditions(testIndex2));
        assertEquals(Collections.emptySet(), diff.getRemovals(testIndex));
        assertEquals(Collections.emptySet(), diff.getRemovals(testIndex2));
        assertTrue(diff.isIndexChanged(testIndex));
        assertTrue(diff.isIndexChanged(testIndex2));
        assertFalse(diff.isIndexChanged(testIndex3));
    }

    @Test
    public void canInsertDeletions() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(new Object(), new Object());
        assertTrue(diff.isEmpty());
        assertFalse(diff.isEntryAddition());
        assertFalse(diff.isEntryRemoval());
        assertTrue(diff.isEntryUpdate());
        DummyIndex testIndex = new DummyIndex(new FirstNameIndexer(), "test");
        DummyIndex testIndex2 = new DummyIndex(new FirstNameIndexer(), "test2");
        DummyIndex testIndex3 = new DummyIndex(new FirstNameIndexer(), "test3");
        diff.removeSingleValue(testIndex, "Hello World");
        diff.removeSingleValue(testIndex, "Foo Bar");
        diff.removeSingleValue(testIndex2, "Baz");
        assertEquals(Sets.newHashSet(testIndex, testIndex2), diff.getChangedIndices());
        assertEquals(Sets.newHashSet("Hello World", "Foo Bar"), diff.getRemovals(testIndex));
        assertEquals(Sets.newHashSet("Baz"), diff.getRemovals(testIndex2));
        assertEquals(Collections.emptySet(), diff.getAdditions(testIndex));
        assertEquals(Collections.emptySet(), diff.getAdditions(testIndex2));
        assertTrue(diff.isIndexChanged(testIndex));
        assertTrue(diff.isIndexChanged(testIndex2));
        assertFalse(diff.isIndexChanged(testIndex3));
    }

    @Test
    public void cannotInsertRemovalsIntoEntryAdditionDiff() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(null, new Object());
        DummyIndex testIndex = new DummyIndex(new FirstNameIndexer(), "test");
        try {
            diff.removeSingleValue(testIndex, "Hello World");
            fail("Managed to remove an index value in an entry addition diff!");
        } catch (Exception ignored) {
            // pass
        }
    }

    @Test
    public void cannotInsertAdditionsIntoEntryRemovalDiff() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(new Object(), null);
        DummyIndex testIndex = new DummyIndex(new FirstNameIndexer(), "test");
        try {
            diff.add(testIndex, "Hello World");
            fail("Managed to add an index value in an entry removal diff!");
        } catch (Exception ignored) {
            // pass
        }
    }

    @Test
    public void insertingAnAdditionOverridesARemoval() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(new Object(), new Object());
        DummyIndex testIndex = new DummyIndex(new FirstNameIndexer(), "test");
        DummyIndex testIndex2 = new DummyIndex(new FirstNameIndexer(), "test2");
        diff.removeSingleValue(testIndex, "Hello World");
        diff.removeSingleValue(testIndex, "Foo Bar");
        assertEquals(Sets.newHashSet(testIndex), diff.getChangedIndices());
        assertEquals(Sets.newHashSet("Hello World", "Foo Bar"), diff.getRemovals(testIndex));
        assertEquals(Collections.emptySet(), diff.getAdditions(testIndex));
        diff.add(testIndex, "Hello World");
        assertEquals(Sets.newHashSet(testIndex), diff.getChangedIndices());
        assertEquals(Sets.newHashSet("Foo Bar"), diff.getRemovals(testIndex));
        assertEquals(Sets.newHashSet("Hello World"), diff.getAdditions(testIndex));
        assertTrue(diff.isIndexChanged(testIndex));
        assertFalse(diff.isIndexChanged(testIndex2));
    }

    @Test
    public void insertingARemovalOverridesAnAddition() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(new Object(), new Object());
        DummyIndex testIndex = new DummyIndex(new FirstNameIndexer(), "test");
        DummyIndex testIndex2 = new DummyIndex(new FirstNameIndexer(), "test2");
        diff.add(testIndex, "Hello World");
        diff.add(testIndex, "Foo Bar");
        assertEquals(Sets.newHashSet(testIndex), diff.getChangedIndices());
        assertEquals(Sets.newHashSet("Hello World", "Foo Bar"), diff.getAdditions(testIndex));
        assertEquals(Collections.emptySet(), diff.getRemovals(testIndex));
        diff.removeSingleValue(testIndex, "Hello World");
        assertEquals(Sets.newHashSet(testIndex), diff.getChangedIndices());
        assertEquals(Sets.newHashSet("Foo Bar"), diff.getAdditions(testIndex));
        assertEquals(Sets.newHashSet("Hello World"), diff.getRemovals(testIndex));
        assertTrue(diff.isIndexChanged(testIndex));
        assertFalse(diff.isIndexChanged(testIndex2));
    }

    @Test
    public void canDetectThatDiffisAdditive() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(new Object(), new Object());
        DummyIndex testIndex = new DummyIndex(new FirstNameIndexer(), "test");
        diff.add(testIndex, "Hello World");
        diff.add(testIndex, "Foo Bar");
        assertTrue(diff.isAdditive());
        assertFalse(diff.isSubtractive());
        assertFalse(diff.isMixed());
        assertFalse(diff.isEmpty());
    }

    @Test
    public void canDetectThatDiffIsSubtractive() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(new Object(), new Object());
        DummyIndex testIndex = new DummyIndex(new FirstNameIndexer(), "test");
        diff.removeSingleValue(testIndex, "Hello World");
        diff.removeSingleValue(testIndex, "Foo Bar");
        assertFalse(diff.isAdditive());
        assertTrue(diff.isSubtractive());
        assertFalse(diff.isMixed());
        assertFalse(diff.isEmpty());
    }

    @Test
    public void canDetectThatDiffIsMixed() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(new Object(), new Object());
        DummyIndex testIndex = new DummyIndex(new FirstNameIndexer(), "test");
        diff.add(testIndex, "Hello World");
        diff.removeSingleValue(testIndex, "Foo Bar");
        assertFalse(diff.isAdditive());
        assertFalse(diff.isSubtractive());
        assertTrue(diff.isMixed());
        assertFalse(diff.isEmpty());
    }
}
