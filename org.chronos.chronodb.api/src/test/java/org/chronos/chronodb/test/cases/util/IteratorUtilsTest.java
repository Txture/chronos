package org.chronos.chronodb.test.cases.util;

import com.google.common.collect.Lists;
import org.chronos.chronodb.internal.util.IteratorUtils;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class IteratorUtilsTest extends ChronoDBUnitTest {

    @Test
    public void uniqueIteratorWorks() {
        List<Integer> values = Lists.newArrayList(1, 2, 3, 1, 3, 2, 4, 5, 5, 5, 2, 1, 3);
        Iterator<Integer> iterator = values.iterator();
        iterator = IteratorUtils.unique(iterator);
        List<Integer> newValues = Lists.newArrayList(iterator);
        assertEquals(5, newValues.size());
        assertTrue(newValues.contains(1));
        assertTrue(newValues.contains(2));
        assertTrue(newValues.contains(3));
        assertTrue(newValues.contains(4));
        assertTrue(newValues.contains(5));
    }

}
