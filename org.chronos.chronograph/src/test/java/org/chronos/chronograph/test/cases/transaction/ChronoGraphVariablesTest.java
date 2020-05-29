package org.chronos.chronograph.test.cases.transaction;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class ChronoGraphVariablesTest extends AllChronoGraphBackendsTest {

    @Test
    public void canReadWriteVariablesInCustomKeyspace(){
        ChronoGraph g = this.getGraph();
        g.variables().set("foo", "bar");
        g.variables().set("math", "pi", 3.1415);
        g.variables().set("math", "zero", 0);
        g.variables().set("math", "one", 1);
        g.variables().set("config", "log", "everything");
        g.variables().set("config", "awesomeness", "yes");

        this.assertCommitAssert(()->{
            assertEquals(3.1415, g.variables().get("math", "pi").orElse(null));
            assertNull(g.variables().get("pi").orElse(null));
            assertNull(g.variables().get("foo", "pi").orElse(null));
            assertNull(g.variables().get("config", "pi").orElse(null));

            assertEquals(0, g.variables().get("math", "zero").orElse(null));
            assertNull(g.variables().get("zero").orElse(null));
            assertNull(g.variables().get("foo", "zero").orElse(null));
            assertNull(g.variables().get("config", "zero").orElse(null));

            assertEquals("everything", g.variables().get("config", "log").orElse(null));
            assertEquals("yes", g.variables().get("config", "awesomeness").orElse(null));

            assertEquals(Sets.newHashSet("foo"), g.variables().keys());
            assertEquals(Sets.newHashSet("pi", "zero", "one"), g.variables().keys("math"));
            assertEquals(Sets.newHashSet("log", "awesomeness"), g.variables().keys("config"));

            assertEquals(Sets.newHashSet(ChronoGraphConstants.VARIABLES_DEFAULT_KEYSPACE, "math", "config"), g.variables().keyspaces());

            Map<String, Object> mathMap = Maps.newHashMap();
            mathMap.put("pi", 3.1415);
            mathMap.put("zero", 0);
            mathMap.put("one", 1);

            assertEquals(mathMap, g.variables().asMap("math"));

            Map<String, Object> configMap = Maps.newHashMap();
            configMap.put("log", "everything");
            configMap.put("awesomeness", "yes");

            assertEquals(configMap, g.variables().asMap("config"));

            Map<String, Object> defaultKeyspaceMap = Maps.newHashMap();
            defaultKeyspaceMap.put("foo", "bar");

            assertEquals(defaultKeyspaceMap, g.variables().asMap());
        });
    }

    @Test
    public void canQueryHistoryOfGraphVariables(){
        ChronoGraph g = this.getGraph();
        g.variables().set("foo", "bar");
        g.variables().set("greeting", "hello", "world");
        long commit1 = g.tx().commitAndReturnTimestamp();
        assertThat(commit1, is(greaterThan(0L)));

        g.variables().set("foo", "baz");
        g.variables().set("person", "john", "doe");
        long commit2 = g.tx().commitAndReturnTimestamp();
        assertThat(commit2, is(greaterThan(commit1)));

        g.variables().set("foo", "blub");
        g.variables().set("fizz", "buzz");
        g.variables().set("greeting", "hello", "chronos");
        long commit3 = g.tx().commitAndReturnTimestamp();
        assertThat(commit3, is(greaterThan(commit2)));

        List<Pair<String, String>> commit2Variables = Lists.newArrayList(g.getChangedGraphVariablesAtCommit(commit2));
        assertThat(commit2Variables, containsInAnyOrder(Pair.of(ChronoGraphConstants.VARIABLES_DEFAULT_KEYSPACE, "foo"), Pair.of("person", "john")));

        List<String> changedVariablesInDefaultKeyspaceAtCommit3 = Lists.newArrayList(g.getChangedGraphVariablesAtCommitInDefaultKeyspace(commit3));
        assertThat(changedVariablesInDefaultKeyspaceAtCommit3, containsInAnyOrder("foo", "fizz"));

        List<String> greetingsChangesInCommit3 = Lists.newArrayList(g.getChangedGraphVariablesAtCommitInKeyspace(commit3, "greeting"));
        assertThat(greetingsChangesInCommit3, containsInAnyOrder("hello"));
    }

    @Test
    public void nonChangesOnVariablesAreIgnoredInDefaultKeyspace(){
        ChronoGraph g = this.getGraph();
        g.variables().set("foo", "bar");
        assertThat(g.tx().commitAndReturnTimestamp(), is(greaterThan(0L)));

        // set the variable to the same value it already has -> non-change
        g.variables().set("foo", "bar");
        assertEquals(-1, g.tx().commitAndReturnTimestamp());

        // set the variable to another value...
        g.variables().set("foo", "blub");
        // ...and back to the original one
        g.variables().set("foo","bar");
        assertEquals(-1, g.tx().commitAndReturnTimestamp());
    }

    @Test
    public void nonChangesOnVariablesAreIgnoredInCustomKeyspace(){
        ChronoGraph g = this.getGraph();
        g.variables().set("custom", "foo", "bar");
        g.tx().commit();

        // set the variable to the same value it already has -> non-change
        g.variables().set("custom", "foo", "bar");
        assertEquals(-1, g.tx().commitAndReturnTimestamp());

        // set the variable to another value...
        g.variables().set("custom", "foo", "blub");
        // ...and back to the original one
        g.variables().set("custom", "foo","bar");
        assertEquals(-1, g.tx().commitAndReturnTimestamp());
    }


}
