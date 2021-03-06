package org.chronos.chronodb.test.cases.engine.indexing.diff;

import com.google.common.collect.Sets;
import org.chronos.chronodb.internal.impl.index.diff.MutableIndexValueDiff;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
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
        diff.add("test", "Hello World");
        diff.add("test", "Foo Bar");
        diff.add("test2", "Baz");
        assertEquals(Sets.newHashSet("test", "test2"), diff.getChangedIndices());
        assertEquals(Sets.newHashSet("Hello World", "Foo Bar"), diff.getAdditions("test"));
        assertEquals(Sets.newHashSet("Baz"), diff.getAdditions("test2"));
        assertEquals(Collections.emptySet(), diff.getRemovals("test"));
        assertEquals(Collections.emptySet(), diff.getRemovals("test2"));
        assertTrue(diff.isIndexChanged("test"));
        assertTrue(diff.isIndexChanged("test2"));
        assertFalse(diff.isIndexChanged("test3"));
    }

    @Test
    public void canInsertDeletions() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(new Object(), new Object());
        assertTrue(diff.isEmpty());
        assertFalse(diff.isEntryAddition());
        assertFalse(diff.isEntryRemoval());
        assertTrue(diff.isEntryUpdate());
        diff.removeSingleValue("test", "Hello World");
        diff.removeSingleValue("test", "Foo Bar");
        diff.removeSingleValue("test2", "Baz");
        assertEquals(Sets.newHashSet("test", "test2"), diff.getChangedIndices());
        assertEquals(Sets.newHashSet("Hello World", "Foo Bar"), diff.getRemovals("test"));
        assertEquals(Sets.newHashSet("Baz"), diff.getRemovals("test2"));
        assertEquals(Collections.emptySet(), diff.getAdditions("test"));
        assertEquals(Collections.emptySet(), diff.getAdditions("test2"));
        assertTrue(diff.isIndexChanged("test"));
        assertTrue(diff.isIndexChanged("test2"));
        assertFalse(diff.isIndexChanged("test3"));
    }

    @Test
    public void cannotInsertRemovalsIntoEntryAdditionDiff() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(null, new Object());
        try {
            diff.removeSingleValue("test", "Hello World");
            fail("Managed to remove an index value in an entry addition diff!");
        } catch (Exception ignored) {
            // pass
        }
    }

    @Test
    public void cannotInsertAdditionsIntoEntryRemovalDiff() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(new Object(), null);
        try {
            diff.add("test", "Hello World");
            fail("Managed to add an index value in an entry removal diff!");
        } catch (Exception ignored) {
            // pass
        }
    }

    @Test
    public void insertingAnAdditionOverridesARemoval() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(new Object(), new Object());
        diff.removeSingleValue("test", "Hello World");
        diff.removeSingleValue("test", "Foo Bar");
        assertEquals(Sets.newHashSet("test"), diff.getChangedIndices());
        assertEquals(Sets.newHashSet("Hello World", "Foo Bar"), diff.getRemovals("test"));
        assertEquals(Collections.emptySet(), diff.getAdditions("test"));
        diff.add("test", "Hello World");
        assertEquals(Sets.newHashSet("test"), diff.getChangedIndices());
        assertEquals(Sets.newHashSet("Foo Bar"), diff.getRemovals("test"));
        assertEquals(Sets.newHashSet("Hello World"), diff.getAdditions("test"));
        assertTrue(diff.isIndexChanged("test"));
        assertFalse(diff.isIndexChanged("test2"));
    }

    @Test
    public void insertingARemovalOverridesAnAddition() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(new Object(), new Object());
        diff.add("test", "Hello World");
        diff.add("test", "Foo Bar");
        assertEquals(Sets.newHashSet("test"), diff.getChangedIndices());
        assertEquals(Sets.newHashSet("Hello World", "Foo Bar"), diff.getAdditions("test"));
        assertEquals(Collections.emptySet(), diff.getRemovals("test"));
        diff.removeSingleValue("test", "Hello World");
        assertEquals(Sets.newHashSet("test"), diff.getChangedIndices());
        assertEquals(Sets.newHashSet("Foo Bar"), diff.getAdditions("test"));
        assertEquals(Sets.newHashSet("Hello World"), diff.getRemovals("test"));
        assertTrue(diff.isIndexChanged("test"));
        assertFalse(diff.isIndexChanged("test2"));
    }

    @Test
    public void canDetectThatDiffisAdditive() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(new Object(), new Object());
        diff.add("test", "Hello World");
        diff.add("test", "Foo Bar");
        assertTrue(diff.isAdditive());
        assertFalse(diff.isSubtractive());
        assertFalse(diff.isMixed());
        assertFalse(diff.isEmpty());
    }

    @Test
    public void canDetectThatDiffIsSubtractive() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(new Object(), new Object());
        diff.removeSingleValue("test", "Hello World");
        diff.removeSingleValue("test", "Foo Bar");
        assertFalse(diff.isAdditive());
        assertTrue(diff.isSubtractive());
        assertFalse(diff.isMixed());
        assertFalse(diff.isEmpty());
    }

    @Test
    public void canDetectThatDiffIsMixed() {
        MutableIndexValueDiff diff = new MutableIndexValueDiff(new Object(), new Object());
        diff.add("test", "Hello World");
        diff.removeSingleValue("test", "Foo Bar");
        assertFalse(diff.isAdditive());
        assertFalse(diff.isSubtractive());
        assertTrue(diff.isMixed());
        assertFalse(diff.isEmpty());
    }
}
