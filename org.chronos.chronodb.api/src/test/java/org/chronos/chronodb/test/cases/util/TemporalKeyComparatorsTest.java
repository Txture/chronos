package org.chronos.chronodb.test.cases.util;

import com.google.common.collect.Lists;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.test.base.ChronoDBUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class TemporalKeyComparatorsTest extends ChronoDBUnitTest {

    @Test
    public void comparingTemporalKeysByTimestampKeyspaceKeyWorks() {
        Comparator<TemporalKey> comparator = TemporalKey.Comparators.BY_TIME_KEYSPACE_KEY;
        TemporalKey tk1 = TemporalKey.create(1000, "default", "test");
        TemporalKey tk2 = TemporalKey.create(900, "default", "test");
        TemporalKey tk3 = TemporalKey.create(1100, "default", "test");
        TemporalKey tk4 = TemporalKey.create(1000, "mykeyspace", "test");
        TemporalKey tk5 = TemporalKey.create(900, "mykeyspace", "test");
        TemporalKey tk6 = TemporalKey.create(1100, "mykeyspace", "test");
        TemporalKey tk7 = TemporalKey.create(1000, "default", "foo");
        TemporalKey tk8 = TemporalKey.create(1000, "default", "a");
        TemporalKey tk9 = TemporalKey.create(1000, "default", "x");

        List<TemporalKey> list = Lists.newArrayList();
        list.add(tk1);
        list.add(tk2);
        list.add(tk3);
        list.add(tk4);
        list.add(tk5);
        list.add(tk6);
        list.add(tk7);
        list.add(tk8);
        list.add(tk9);
        Collections.sort(list, comparator);

        assertEquals(Lists.newArrayList(tk2, tk5, tk8, tk7, tk1, tk9, tk4, tk3, tk6), list);
    }
}
