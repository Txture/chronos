package org.chronos.chronodb.test.cases.engine.indexing.diff;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.internal.impl.index.diff.IndexValueDiff;
import org.chronos.chronodb.internal.impl.index.diff.IndexingUtils;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.chronodb.test.cases.util.model.person.PersonIndexer;
import org.chronos.common.test.junit.categories.UnitTest;
import org.chronos.common.test.utils.model.person.Person;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class IndexingUtilsTest extends ChronoDBUnitTest {

    // =====================================================================================================================
    // GET INDEX VALUES TESTS
    // =====================================================================================================================

    @Test
    public void canGetIndexValuesForObject() {
        Person johnDoe = createJohnDoe();
        Collection<Object> firstNameValues = IndexingUtils.getIndexedValuesForObject(
            Collections.singleton(new DummyIndex(PersonIndexer.firstName())),
            johnDoe
        ).values();
        assertEquals(Sets.newHashSet("John"), Sets.newHashSet(firstNameValues));
        Collection<Object> lastNameValues = IndexingUtils.getIndexedValuesForObject(
            Collections.singleton(new DummyIndex(PersonIndexer.lastName())),
            johnDoe
        ).values();
        assertEquals(Sets.newHashSet("Doe"), Sets.newHashSet(lastNameValues));
    }

    @Test
    public void canGetIndexValuesForNullValuedFields() {
        Person johnDoe = createJohnDoe();
        Collection<Object> favoriteColorValues = IndexingUtils.getIndexedValuesForObject(
            Collections.singleton(new DummyIndex(PersonIndexer.favoriteColor())),
            johnDoe
        ).values();
        assertEquals(Collections.emptySet(), Sets.newHashSet(favoriteColorValues));
    }

    @Test
    public void canGetIndexValuesForEmptyCollectionFields() {
        Person johnDoe = createJohnDoe();
        Collection<Object> petsValues = IndexingUtils.getIndexedValuesForObject(
            Collections.singleton(new DummyIndex(PersonIndexer.pets())),
            johnDoe
        ).values();
        assertEquals(Collections.emptySet(), Sets.newHashSet(petsValues));
    }

    @Test
    public void indexedValuesDoNotContainNullOrEmptyString() {
        Person johnDoe = createJohnDoe();
        Collection<Object> hobbiesValues = IndexingUtils.getIndexedValuesForObject(
            Collections.singleton(new DummyIndex(PersonIndexer.hobbies())),
            johnDoe
        ).values();
        assertEquals(Sets.newHashSet("Swimming", "Skiing"), Sets.newHashSet(hobbiesValues));
    }

    @Test
    public void attemptingToGetIndexValuesWithoutIndexerProducesEmptySet() {
        Person johnDoe = createJohnDoe();
        Collection<Object> hobbiesValues2 = IndexingUtils.getIndexedValuesForObject(
            Collections.emptySet(),
            johnDoe
        ).values();
        assertEquals(Collections.emptySet(),  Sets.newHashSet(hobbiesValues2));
    }

    // =====================================================================================================================
    // DIFF CALCULATION TESTS
    // =====================================================================================================================

    @Test
    public void canCalculateAdditiveDiff() {
        Person p1 = new Person();
        p1.setFirstName("John");
        p1.setLastName("Doe");
        p1.setHobbies("Swimming", "Skiing");
        Person p2 = new Person();
        p2.setFirstName("John");
        p2.setLastName("Doe");
        p2.setHobbies("Swimming", "Skiing", "Cinema", "Reading");
        p2.setPets("Cat", "Dog", "Fish");
        Set<SecondaryIndex> indices = Sets.newHashSet();
        DummyIndex firstNameIndex = new DummyIndex(PersonIndexer.firstName(), "firstName");
        DummyIndex lastNameIndex = new DummyIndex(PersonIndexer.lastName(), "lastName");
        DummyIndex hobbiesIndex = new DummyIndex(PersonIndexer.hobbies(), "hobbies");
        DummyIndex petsIndex = new DummyIndex(PersonIndexer.pets(), "pets");
        indices.add(firstNameIndex);
        indices.add(lastNameIndex);
        indices.add(hobbiesIndex);
        indices.add(petsIndex);
        IndexValueDiff diff = IndexingUtils.calculateDiff(indices, p1, p2);
        assertNotNull(diff);
        assertFalse(diff.isEntryAddition());
        assertFalse(diff.isEntryRemoval());
        assertTrue(diff.isEntryUpdate());
        assertTrue(diff.isAdditive());
        assertFalse(diff.isSubtractive());
        assertFalse(diff.isMixed());
        assertFalse(diff.isEmpty());
        assertEquals(Sets.newHashSet(hobbiesIndex, petsIndex), diff.getChangedIndices());
        assertEquals(Sets.newHashSet("Cinema", "Reading"), diff.getAdditions(hobbiesIndex));
        assertEquals(Sets.newHashSet("Cat", "Dog", "Fish"), diff.getAdditions(petsIndex));
    }

    @Test
    public void canCalculateSubtractiveDiff() {
        Person p1 = new Person();
        p1.setFirstName("John");
        p1.setLastName("Doe");
        p1.setHobbies("Swimming", "Skiing", "Cinema", "Reading");
        p1.setPets("Cat", "Dog", "Fish");
        Person p2 = new Person();
        p2.setFirstName("John");
        p2.setLastName("Doe");
        p2.setHobbies("Swimming", "Skiing");
        Set<SecondaryIndex> indices = Sets.newHashSet();
        DummyIndex firstNameIndex = new DummyIndex(PersonIndexer.firstName(), "firstName");
        DummyIndex lastNameIndex = new DummyIndex(PersonIndexer.lastName(), "lastName");
        DummyIndex hobbiesIndex = new DummyIndex(PersonIndexer.hobbies(), "hobbies");
        DummyIndex petsIndex = new DummyIndex(PersonIndexer.pets(), "pets");
        indices.add(firstNameIndex);
        indices.add(lastNameIndex);
        indices.add(hobbiesIndex);
        indices.add(petsIndex);
        IndexValueDiff diff = IndexingUtils.calculateDiff(indices, p1, p2);
        assertNotNull(diff);
        assertFalse(diff.isEntryAddition());
        assertFalse(diff.isEntryRemoval());
        assertTrue(diff.isEntryUpdate());
        assertFalse(diff.isAdditive());
        assertTrue(diff.isSubtractive());
        assertFalse(diff.isMixed());
        assertFalse(diff.isEmpty());
        assertEquals(Sets.newHashSet(hobbiesIndex, petsIndex), diff.getChangedIndices());
        assertEquals(Sets.newHashSet("Cinema", "Reading"), diff.getRemovals(hobbiesIndex));
        assertEquals(Sets.newHashSet("Cat", "Dog", "Fish"), diff.getRemovals(petsIndex));
    }

    @Test
    public void canCalculateEntryAdditionDiff() {
        Person johnDoe = createJohnDoe();
        Set<SecondaryIndex> indices = Sets.newHashSet();
        DummyIndex firstNameIndex = new DummyIndex(PersonIndexer.firstName(), "firstName");
        DummyIndex lastNameIndex = new DummyIndex(PersonIndexer.lastName(), "lastName");
        DummyIndex favoriteColorIndex = new DummyIndex(PersonIndexer.favoriteColor(), "favoriteColor");
        DummyIndex hobbiesIndex = new DummyIndex(PersonIndexer.hobbies(), "hobbies");
        DummyIndex petsIndex = new DummyIndex(PersonIndexer.pets(), "pets");
        indices.add(firstNameIndex);
        indices.add(lastNameIndex);
        indices.add(favoriteColorIndex);
        indices.add(hobbiesIndex);
        indices.add(petsIndex);

        // simulate the addition of John Doe
        IndexValueDiff diff = IndexingUtils.calculateDiff(indices, null, johnDoe);
        assertNotNull(diff);
        assertTrue(diff.isEntryAddition());
        assertFalse(diff.isEntryRemoval());
        assertFalse(diff.isEntryUpdate());
        assertTrue(diff.isAdditive());
        assertFalse(diff.isSubtractive());
        assertFalse(diff.isMixed());
        assertFalse(diff.isEmpty());
        assertEquals(Sets.newHashSet(firstNameIndex, lastNameIndex, hobbiesIndex), diff.getChangedIndices());

        assertEquals(Sets.newHashSet("John"), diff.getAdditions(firstNameIndex));
        assertEquals(Collections.emptySet(), diff.getRemovals(firstNameIndex));

        assertEquals(Sets.newHashSet("Doe"), diff.getAdditions(lastNameIndex));
        assertEquals(Collections.emptySet(), diff.getRemovals(lastNameIndex));

        assertEquals(Sets.newHashSet("Swimming", "Skiing"), diff.getAdditions(hobbiesIndex));
        assertEquals(Collections.emptySet(), diff.getRemovals(hobbiesIndex));
    }

    @Test
    public void canCalculateEntryRemovalDiff() {
        Person johnDoe = createJohnDoe();
        Set<SecondaryIndex> indices = Sets.newHashSet();
        DummyIndex firstNameIndex = new DummyIndex(PersonIndexer.firstName(), "firstName");
        DummyIndex lastNameIndex = new DummyIndex(PersonIndexer.lastName(), "lastName");
        DummyIndex favoriteColorIndex = new DummyIndex(PersonIndexer.favoriteColor(), "favoriteColor");
        DummyIndex hobbiesIndex = new DummyIndex(PersonIndexer.hobbies(), "hobbies");
        DummyIndex petsIndex = new DummyIndex(PersonIndexer.pets(), "pets");
        indices.add(firstNameIndex);
        indices.add(lastNameIndex);
        indices.add(favoriteColorIndex);
        indices.add(hobbiesIndex);
        indices.add(petsIndex);
        // simulate the deletion of John Doe
        IndexValueDiff diff = IndexingUtils.calculateDiff(indices, johnDoe, null);
        assertNotNull(diff);
        assertFalse(diff.isEntryAddition());
        assertTrue(diff.isEntryRemoval());
        assertFalse(diff.isEntryUpdate());
        assertFalse(diff.isAdditive());
        assertTrue(diff.isSubtractive());
        assertFalse(diff.isMixed());
        assertFalse(diff.isEmpty());
        assertEquals(Sets.newHashSet(firstNameIndex, lastNameIndex, hobbiesIndex), diff.getChangedIndices());

        assertEquals(Collections.emptySet(), diff.getAdditions(firstNameIndex));
        assertEquals(Sets.newHashSet("John"), diff.getRemovals(firstNameIndex));

        assertEquals(Collections.emptySet(), diff.getAdditions(lastNameIndex));
        assertEquals(Sets.newHashSet("Doe"), diff.getRemovals(lastNameIndex));

        assertEquals(Collections.emptySet(), diff.getAdditions(hobbiesIndex));
        assertEquals(Sets.newHashSet("Swimming", "Skiing"), diff.getRemovals(hobbiesIndex));
    }

    @Test
    public void canCalculateMixedDiff() {
        Person p1 = new Person();
        p1.setFirstName("John");
        p1.setLastName("Doe");
        p1.setHobbies("Swimming", "Skiing");
        Person p2 = new Person();
        p2.setFirstName("John");
        p2.setLastName("Smith");
        p2.setHobbies("Skiing", "Cinema");
        Set<SecondaryIndex> indices = Sets.newHashSet();
        DummyIndex firstNameIndex = new DummyIndex(PersonIndexer.firstName(), "firstName");
        DummyIndex lastNameIndex = new DummyIndex(PersonIndexer.lastName(), "lastName");
        DummyIndex hobbiesIndex = new DummyIndex(PersonIndexer.hobbies(), "hobbies");
        indices.add(firstNameIndex);
        indices.add(lastNameIndex);
        indices.add(hobbiesIndex);
        IndexValueDiff diff = IndexingUtils.calculateDiff(indices, p1, p2);
        assertNotNull(diff);
        assertFalse(diff.isEntryAddition());
        assertFalse(diff.isEntryRemoval());
        assertTrue(diff.isEntryUpdate());
        assertFalse(diff.isAdditive());
        assertFalse(diff.isSubtractive());
        assertTrue(diff.isMixed());
        assertFalse(diff.isEmpty());
        assertEquals(Collections.emptySet(), diff.getAdditions(firstNameIndex));
        assertEquals(Collections.emptySet(), diff.getRemovals(firstNameIndex));
        assertFalse(diff.isIndexChanged(firstNameIndex));
        assertEquals(Collections.singleton("Smith"), diff.getAdditions(lastNameIndex));
        assertEquals(Collections.singleton("Doe"), diff.getRemovals(lastNameIndex));
        assertTrue(diff.isIndexChanged(lastNameIndex));
        assertEquals(Collections.singleton("Cinema"), diff.getAdditions(hobbiesIndex));
        assertEquals(Collections.singleton("Swimming"), diff.getRemovals(hobbiesIndex));
        assertTrue(diff.isIndexChanged(hobbiesIndex));
        assertEquals(Sets.newHashSet(lastNameIndex, hobbiesIndex), diff.getChangedIndices());
        assertFalse(diff.isEntryAddition());
        assertFalse(diff.isEntryRemoval());
        assertTrue(diff.isEntryUpdate());
    }

    // =====================================================================================================================
    // INTERNAL HELPER METHODS
    // =====================================================================================================================

    private static Person createJohnDoe() {
        Person johnDoe = new Person();
        johnDoe.setFirstName("John");
        johnDoe.setLastName("Doe");
        johnDoe.setFavoriteColor(null);
        johnDoe.setHobbies(Sets.newHashSet("Swimming", "Skiing", "   ", "", null));
        johnDoe.setPets(Collections.emptySet());
        return johnDoe;
    }
}
