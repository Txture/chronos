package org.chronos.chronograph.test.cases.transaction.trigger;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Graph.Variables;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronograph.api.exceptions.TriggerAlreadyExistsException;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.structure.ElementLifecycleStatus;
import org.chronos.chronograph.api.transaction.trigger.CancelCommitException;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPostCommitTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPostPersistTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPreCommitTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPrePersistTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphTrigger;
import org.chronos.chronograph.api.transaction.trigger.GraphTriggerMetadata;
import org.chronos.chronograph.api.transaction.trigger.PostCommitTriggerContext;
import org.chronos.chronograph.api.transaction.trigger.PostPersistTriggerContext;
import org.chronos.chronograph.api.transaction.trigger.PreCommitTriggerContext;
import org.chronos.chronograph.api.transaction.trigger.PrePersistTriggerContext;
import org.chronos.chronograph.api.transaction.trigger.TriggerContext;
import org.chronos.chronograph.api.transaction.trigger.TriggerTiming;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.internal.impl.transaction.trigger.ChronoGraphTriggerManagerInternal;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.chronograph.test.cases.transaction.trigger.ChronoGraphTriggerTest.AppendPriorityTrigger.AppendPriorityPreCommitTrigger;
import org.chronos.chronograph.test.cases.transaction.trigger.ChronoGraphTriggerTest.AppendPriorityTrigger.AppendPriorityPrePersistTrigger;
import org.chronos.chronograph.test.cases.transaction.trigger.ChronoGraphTriggerTest.ChangeSetRecordingTrigger.ChangeSetRecordingPostCommitTrigger;
import org.chronos.chronograph.test.cases.transaction.trigger.ChronoGraphTriggerTest.ChangeSetRecordingTrigger.ChangeSetRecordingPostPersistTrigger;
import org.chronos.chronograph.test.cases.transaction.trigger.ChronoGraphTriggerTest.VertexCountTrigger.VertexCountPostCommitTrigger;
import org.chronos.chronograph.test.cases.transaction.trigger.ChronoGraphTriggerTest.VertexCountTrigger.VertexCountPostPersistTrigger;
import org.chronos.chronograph.test.cases.transaction.trigger.ChronoGraphTriggerTest.VertexCountTrigger.VertexCountPreCommitTrigger;
import org.chronos.chronograph.test.cases.transaction.trigger.ChronoGraphTriggerTest.VertexCountTrigger.VertexCountPrePersistTrigger;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

public class ChronoGraphTriggerTest extends AllChronoGraphBackendsTest {

    @Test
    public void cannotAddTriggerWithoutConcreteInterface(){
        ChronoGraph graph = this.getGraph();
        try{
            graph.getTriggerManager().createTrigger("test", new FakeTrigger());
            fail("Managed to add a trigger without a concrete interface!");
        }catch(IllegalArgumentException expected){
            // pass
        }
        try{
            graph.getTriggerManager().createTriggerIfNotPresent("test", FakeTrigger::new);
            fail("Managed to add a trigger without a concrete interface!");
        }catch(IllegalArgumentException expected){
            // pass
        }
        try{
            graph.getTriggerManager().createTriggerAndOverwrite("test", new FakeTrigger());
            fail("Managed to add a trigger without a concrete interface!");
        }catch(IllegalArgumentException expected){
            // pass
        }
    }

    @Test
    public void canAddRemoveAndListTriggers() {
        ChronoGraph graph = this.getGraph();
        graph.getTriggerManager().createTrigger("foo", new DummyTrigger());
        graph.getTriggerManager().createTrigger("bar", new DummyTrigger());
        assertThat(graph.getTriggerManager().existsTrigger("foo"), is(true));
        assertThat(graph.getTriggerManager().existsTrigger("bar"), is(true));
        assertThat(graph.getTriggerManager().getTriggerNames(), containsInAnyOrder("foo", "bar"));

        graph.getTriggerManager().dropTrigger("foo");
        assertThat(graph.getTriggerManager().existsTrigger("foo"), is(false));
        assertThat(graph.getTriggerManager().existsTrigger("bar"), is(true));
        assertThat(graph.getTriggerManager().getTriggerNames(), containsInAnyOrder("bar"));
    }

    @Test
    public void cannotAddTwoTriggersWithTheSameName() {
        ChronoGraph graph = this.getGraph();
        graph.getTriggerManager().createTrigger("foo", new DummyTrigger());

        try {
            graph.getTriggerManager().createTrigger("foo", new DummyTrigger());
            fail("Managed to add two triggers with the same name!");
        } catch (TriggerAlreadyExistsException expected) {
            // pass
        }
    }

    @Test
    public void canOverrideTriggerByName() {
        ChronoGraph graph = this.getGraph();
        graph.getTriggerManager().createTrigger("foo", new DummyTrigger());
        graph.getTriggerManager().createTriggerAndOverwrite("foo", new DummyTrigger2());
        assertThat(graph.getTriggerManager().existsTrigger("foo"), is(true));
        assertThat(graph.getTriggerManager().getTriggerNames(), contains("foo"));

        ChronoGraphTriggerManagerInternal triggerManager = (ChronoGraphTriggerManagerInternal) graph.getTriggerManager();
        List<Pair<String, ChronoGraphPostCommitTrigger>> triggers = triggerManager.getPostCommitTriggers();
        assertThat(triggers.size(), is(1));
        Pair<String, ChronoGraphPostCommitTrigger> pair = Iterables.getOnlyElement(triggers);
        assertThat(pair, is(notNullValue()));
        assertThat(pair.getLeft(), is("foo"));
        assertThat(pair.getRight(), is(notNullValue()));
        assertThat(pair.getRight(), instanceOf(DummyTrigger2.class));
    }

    @Test
    public void canAddTriggerIfNotPresent() {
        ChronoGraph graph = this.getGraph();
        boolean added1 = graph.getTriggerManager().createTriggerIfNotPresent("foo", DummyTrigger::new);
        boolean added2 = graph.getTriggerManager().createTriggerIfNotPresent("foo", DummyTrigger2::new);
        assertThat(added1, is(true));
        assertThat(added2, is(false));
        assertThat(graph.getTriggerManager().existsTrigger("foo"), is(true));
        assertThat(graph.getTriggerManager().getTriggerNames(), contains("foo"));

        ChronoGraphTriggerManagerInternal triggerManager = (ChronoGraphTriggerManagerInternal) graph.getTriggerManager();
        List<Pair<String, ChronoGraphPostCommitTrigger>> triggers = triggerManager.getPostCommitTriggers();
        assertThat(triggers.size(), is(1));
        Pair<String, ChronoGraphPostCommitTrigger> pair = Iterables.getOnlyElement(triggers);
        assertThat(pair, is(notNullValue()));
        assertThat(pair.getLeft(), is("foo"));
        assertThat(pair.getRight(), is(notNullValue()));
        assertThat(pair.getRight(), instanceOf(DummyTrigger.class));
    }

    @Test
    public void triggersArePersistedAndRemainAfterRestart() {
        // don't execute this test on the in-memory backend
        assumeThat(((ChronoGraphInternal) this.getGraph()).getBackingDB().getFeatures().isPersistent(), is(true));
        ChronoGraph graph = this.getGraph();
        graph.getTriggerManager().createTrigger("test", new DummyTrigger());
        assertThat(graph.getTriggerManager().getTriggerNames(), contains("test"));

        ChronoGraph reloadedGraph = this.closeAndReopenGraph();
        assertThat(reloadedGraph.getTriggerManager().getTriggerNames(), contains("test"));
        ChronoGraphTriggerManagerInternal triggerManager = (ChronoGraphTriggerManagerInternal) reloadedGraph.getTriggerManager();
        List<Pair<String, ChronoGraphPostCommitTrigger>> triggers = triggerManager.getPostCommitTriggers();
        assertThat(triggers.size(), is(1));
        Pair<String, ChronoGraphPostCommitTrigger> onlyTrigger = Iterables.getOnlyElement(triggers);
        assertThat(onlyTrigger.getLeft(), is("test"));
        assertThat(onlyTrigger.getRight(), is(notNullValue()));
        assertThat(onlyTrigger.getRight(), instanceOf(DummyTrigger.class));
    }

    @Test
    public void triggersAreFiredInCorrectTimingOrder() {
        ChronoGraph graph = this.getGraph();
        graph.getTriggerManager().createTrigger("test", new CallTrackingTrigger());

        graph.tx().open();
        graph.addVertex(T.id, "123");
        graph.tx().commit();

        graph.tx().open();
        graph.addVertex(T.id, "456");
        graph.tx().commit();

        ChronoGraphTriggerManagerInternal triggerManager = (ChronoGraphTriggerManagerInternal) graph.getTriggerManager();
        List<Pair<String, ChronoGraphPostCommitTrigger>> triggers = triggerManager.getPostCommitTriggers();
        assertThat(triggers.size(), is(1));
        Pair<String, ChronoGraphPostCommitTrigger> onlyTrigger = Iterables.getOnlyElement(triggers);
        assertThat(onlyTrigger.getLeft(), is("test"));
        assertThat(onlyTrigger.getRight(), is(notNullValue()));
        assertThat(onlyTrigger.getRight(), instanceOf(CallTrackingTrigger.class));
        CallTrackingTrigger trackingTrigger = (CallTrackingTrigger) onlyTrigger.getRight();
        assertThat(trackingTrigger.getTrackedTimings(), contains(
            // commit 1
            TriggerTiming.PRE_COMMIT,
            TriggerTiming.PRE_PERSIST,
            TriggerTiming.POST_PERSIST,
            TriggerTiming.POST_COMMIT,
            // commit 2
            TriggerTiming.PRE_COMMIT,
            TriggerTiming.PRE_PERSIST,
            TriggerTiming.POST_PERSIST,
            TriggerTiming.POST_COMMIT
        ));
    }

    @Test
    public void txGraphIsModifiableInPreCommitTriggers() {
        ChronoGraph graph = this.getGraph();
        graph.getTriggerManager().createTrigger("test", new VertexCountPreCommitTrigger());

        graph.tx().open();
        graph.addVertex(T.id, "123");
        graph.addVertex(T.id, "456");
        graph.tx().commit();

        { // check the "count" variable
            graph.tx().open();
            Optional<Object> variable = graph.variables().get("vertexCount");
            assertThat(variable, is(notNullValue()));
            assertThat(variable.isPresent(), is(true));
            assertThat(variable.get(), is(2));
            graph.tx().rollback();
        }

        // do another commit that deletes a vertex
        graph.tx().open();
        graph.vertices("123").next().remove();
        graph.tx().commit();

        { // check the "count" variable one more time
            graph.tx().open();
            Optional<Object> variable = graph.variables().get("vertexCount");
            assertThat(variable, is(notNullValue()));
            assertThat(variable.isPresent(), is(true));
            assertThat(variable.get(), is(1));
            graph.tx().rollback();
        }
    }

    @Test
    public void preCommitTriggerSeesUnmergedState() {
        ChronoGraph graph = this.getGraph();
        graph.getTriggerManager().createTrigger("test", new VertexCountPreCommitTrigger());

        graph.tx().open();
        graph.addVertex(T.id, "123");
        graph.addVertex(T.id, "456");
        graph.tx().commit();

        { // check the "count" variable
            graph.tx().open();
            Optional<Object> variable = graph.variables().get("vertexCount");
            assertThat(variable, is(notNullValue()));
            assertThat(variable.isPresent(), is(true));
            assertThat(variable.get(), is(2));
            graph.tx().rollback();
        }

        // start two transactions: one deletes a vertex, the other one adds two of them.
        // the transaction which deletes the vertex commits first...
        // ... and the other one should see 4 vertices (the deletion has not yet been merged)

        ChronoGraph graphA = graph.tx().createThreadedTx();
        ChronoGraph graphB = graph.tx().createThreadedTx();

        graphA.vertices("123").next().remove();
        graphA.tx().commit();

        graphB.addVertex(T.id, "foo");
        graphB.addVertex(T.id, "bar");
        graphB.tx().commit();

        { // check the "count" variable one more time
            graph.tx().open();
            Optional<Object> variable = graph.variables().get("vertexCount");
            assertThat(variable, is(notNullValue()));
            assertThat(variable.isPresent(), is(true));
            assertThat(variable.get(), is(4));
            graph.tx().rollback();
            // check the actual vertex count (which should be 3)
            assertThat(Iterators.size(graph.vertices()), is(3));
        }

    }

    @Test
    public void prePersistTriggerSeesMergedState() {
        ChronoGraph graph = this.getGraph();
        graph.getTriggerManager().createTrigger("test", new VertexCountPrePersistTrigger());

        graph.tx().open();
        graph.addVertex(T.id, "123");
        graph.addVertex(T.id, "456");
        graph.tx().commit();

        { // check the "count" variable
            graph.tx().open();
            Optional<Object> variable = graph.variables().get("vertexCount");
            assertThat(variable, is(notNullValue()));
            assertThat(variable.isPresent(), is(true));
            assertThat(variable.get(), is(2));
            graph.tx().rollback();
        }

        // start two transactions: one deletes a vertex, the other one adds two of them.
        // the transaction which deletes the vertex commits first...
        // ... and the other one should see 3 vertices (the deletion has been merged in PRE_PERSIST)

        ChronoGraph graphA = graph.tx().createThreadedTx();
        ChronoGraph graphB = graph.tx().createThreadedTx();

        graphA.vertices("123").next().remove();
        graphA.tx().commit();

        graphB.addVertex(T.id, "foo");
        graphB.addVertex(T.id, "bar");
        graphB.tx().commit();

        { // check the "count" variable one more time
            graph.tx().open();
            Optional<Object> variable = graph.variables().get("vertexCount");
            assertThat(variable, is(notNullValue()));
            assertThat(variable.isPresent(), is(true));
            assertThat(variable.get(), is(3));
            graph.tx().rollback();
            // check the actual vertex count
            assertThat(Iterators.size(graph.vertices()), is(3));
        }
    }

    @Test
    public void postPersistTriggersHaveAccessToTheChangeSet() {
        ChronoGraph graph = this.getGraph();
        graph.getTriggerManager().createTrigger("test1", new ChangeSetRecordingPostPersistTrigger());
        graph.getTriggerManager().createTrigger("test2", new ChangeSetRecordingPostCommitTrigger());

        graph.tx().open();
        Vertex v123 = graph.addVertex(T.id, "123");
        Vertex v456 = graph.addVertex(T.id, "456");
        v123.addEdge("test", v456, T.id, "e1");
        graph.variables().set("foo", "bar");
        graph.tx().commit();

        ChronoGraphTriggerManagerInternal triggerManager = (ChronoGraphTriggerManagerInternal) graph.getTriggerManager();
        List<Pair<String, ChronoGraphPostPersistTrigger>> postPersistTriggers = triggerManager.getPostPersistTriggers();
        assertThat(postPersistTriggers.size(), is(1));

        ChronoGraphTrigger trigger1 = Iterables.getOnlyElement(postPersistTriggers).getRight();
        assertThat(trigger1, instanceOf(ChangeSetRecordingTrigger.class));
        ChangeSetRecordingTrigger csrTrigger1 = (ChangeSetRecordingTrigger) trigger1;
        assertThat(csrTrigger1.observedModifiedVertexIds, contains("123", "456"));
        assertThat(csrTrigger1.observedModifiedEdgeIds, contains("e1"));
        assertThat(csrTrigger1.observedModifiedGraphVariableKeys, contains("foo"));

        List<Pair<String, ChronoGraphPostCommitTrigger>> postCommitTriggers = triggerManager.getPostCommitTriggers();
        assertThat(postCommitTriggers.size(), is(1));

        ChronoGraphTrigger trigger2 = Iterables.getOnlyElement(postCommitTriggers).getRight();
        assertThat(trigger2, instanceOf(ChangeSetRecordingTrigger.class));
        ChangeSetRecordingTrigger csrTrigger2 = (ChangeSetRecordingTrigger) trigger2;
        assertThat(csrTrigger2.observedModifiedVertexIds, contains("123", "456"));
        assertThat(csrTrigger2.observedModifiedEdgeIds, contains("e1"));
        assertThat(csrTrigger2.observedModifiedGraphVariableKeys, contains("foo"));
    }

    @Test
    public void postPersistTriggersCannotModifyTheTxGraph() {
        // note: this test will produce exception stack traces in the log output. This is expected and intentional,
        // as ChronoGraph is set up to complain LOUDLY when a change operation is attempted on a read-only graph.
        ChronoGraph graph = this.getGraph();
        graph.getTriggerManager().createTrigger("test", new VertexCountPostPersistTrigger());
        graph.getTriggerManager().createTrigger("test2", new VertexCountPostCommitTrigger());

        graph.tx().open();
        graph.addVertex(T.id, "123");
        graph.addVertex(T.id, "456");
        graph.tx().commit();

        // make sure that the triggers have been unable to create and/or update the target variable
        graph.tx().open();
        Optional<Object> vertexCount = graph.variables().get("vertexCount");
        assertThat(vertexCount.isPresent(), is(false));

        // ... but the commit should be successful, so the vertices should be there
        Set<String> vertexIds = Sets.newHashSet(graph.vertices()).stream().map(v -> (String) v.id()).collect(Collectors.toSet());
        assertThat(vertexIds, containsInAnyOrder("123", "456"));
    }

    @Test
    public void triggerPriorityIsRespected() {
        ChronoGraph graph = this.getGraph();
        graph.getTriggerManager().createTrigger("test1", new AppendPriorityPreCommitTrigger(100));
        graph.getTriggerManager().createTrigger("test2", new AppendPriorityPreCommitTrigger(50));
        graph.getTriggerManager().createTrigger("test3", new AppendPriorityPrePersistTrigger(75));

        graph.tx().open();
        graph.addVertex(T.id, "123");
        graph.tx().commit();

        graph.tx().open();
        Optional<Object> priority = graph.variables().get("priority");
        assertThat(priority, is(notNullValue()));
        assertThat(priority.isPresent(), is(true));
        assertThat(priority.get(), is("100,50,75"));
    }

    @Test
    public void triggersCanWorkOnAPerBranchBasis() {
        ChronoGraph graph = this.getGraph();
        graph.getTriggerManager().createTrigger("test", new OnlyMasterBranchTrigger());

        graph.tx().open();
        graph.addVertex(T.id, "123");
        graph.tx().commit();

        graph.getBranchManager().createBranch("my-branch");
        graph.tx().open("my-branch");
        graph.addVertex(T.id, "456");
        graph.tx().commit();

        graph.tx().open();
        graph.addVertex(T.id, "456");
        graph.tx().commit();

        graph.tx().open("my-branch");
        assertThat(graph.variables().get("count").get(), is(1));
        graph.tx().rollback();

        graph.tx().open();
        assertThat(graph.variables().get("count").get(), is(2));
        graph.tx().rollback();
    }

    @Test
    public void transactionControlIsLockedInTriggers() {
        ChronoGraph graph = this.getGraph();
        graph.getTriggerManager().createTrigger("test", new RollbackTrigger());
        graph.tx().open();
        graph.addVertex(T.id, "123");
        graph.tx().commit();

        graph.tx().open();
        assertThat(Sets.newHashSet(graph.vertices()).stream().map(v -> (String) v.id()).collect(Collectors.toSet()), contains("123"));
        graph.tx().close();
    }

    @Test
    public void postPersistTriggersCanDetectDeletions() {
        ChronoGraph graph = this.getGraph();
        graph.getTriggerManager().createTrigger("test", new OnDeleteTrigger());

        graph.tx().open();
        graph.addVertex(T.id, "123");
        graph.addVertex(T.id, "456");
        // this creates an obsolete vertex (added and removed within same TX)
        graph.addVertex(T.id, "789").remove();
        graph.tx().commit();

        graph.tx().open();
        graph.vertices("123").next().remove();
        graph.addVertex("abc");
        graph.vertices("456").next().remove();
        graph.tx().commit();

        ChronoGraphTriggerManagerInternal triggerManager = (ChronoGraphTriggerManagerInternal) graph.getTriggerManager();
        List<Pair<String, ChronoGraphPostPersistTrigger>> triggers = triggerManager.getPostPersistTriggers();
        assertThat(triggers.size(), is(1));
        OnDeleteTrigger trigger = (OnDeleteTrigger) Iterables.getOnlyElement(triggers).getRight();
        assertThat(trigger.deletedVertexIds, containsInAnyOrder("123", "456"));
    }

    @Test
    public void canAccessBranchAndTimestampInTrigger(){
        ChronoGraph graph = this.getGraph();
        AccessBranchAndTimestampTrigger trigger = new AccessBranchAndTimestampTrigger();
        graph.getTriggerManager().createTrigger("test", trigger);
        graph.tx().open();
        graph.addVertex();
        graph.tx().commit();
        assertThat(trigger.getExceptions(), is(empty()));
    }

    @Test
    public void canAccessPreCommitStateInPostPersistTrigger(){
        ChronoGraph graph = this.getGraph();

        graph.getTriggerManager().createTrigger("test", new PreCommitStoreStateChecker());

        graph.tx().open();
        graph.addVertex(T.id, "hello");
        graph.tx().commit();
        assertThat(PreCommitStoreStateChecker.RECORDED_VERTEX_IDS, is(empty()));


        graph.tx().open();
        graph.addVertex(T.id, "world");
        graph.tx().commit();

        assertThat(PreCommitStoreStateChecker.RECORDED_VERTEX_IDS, contains("hello"));
    }

    @Test
    public void canDetectVertexDeletionsInPostPersistTrigger(){
        ChronoGraph graph = this.getGraph();

        graph.getTriggerManager().createTrigger("test", new PostPersistDeletionFinder());

        graph.tx().open();
        graph.addVertex(T.id, "hello");
        graph.addVertex(T.id, "world");
        graph.tx().commit();

        assertThat(PostPersistDeletionFinder.DETECTED_DELETIONS, is(empty()));

        graph.tx().open();
        graph.vertex("hello").remove();
        graph.tx().commit();

        assertThat(PostPersistDeletionFinder.DETECTED_DELETIONS, contains("hello"));
    }

    @Test
    public void canGetGraphTriggerMetadata(){
        ChronoGraph graph = this.getGraph();

        ChronoGraphTrigger trigger = new DummyTrigger();
        graph.getTriggerManager().createTrigger("dummy", trigger);

        GraphTriggerMetadata metadata = graph.getTriggerManager().getTrigger("dummy");

        assertThat(metadata.getTriggerClassName(), is(DummyTrigger.class.getName()));
        assertThat(metadata.getUserScriptContent(), is(nullValue()));
        assertThat(metadata.getTriggerName(), is("dummy"));
        assertThat(metadata.getPriority(), is(0));
        assertThat(metadata.isPreCommitTrigger(), is(false));
        assertThat(metadata.isPrePersistTrigger(), is(false));
        assertThat(metadata.isPostPersistTrigger(), is(false));
        assertThat(metadata.isPostCommitTrigger(), is(true));
        assertThat(metadata.getInstantiationException(), is(nullValue()));
    }

    // =================================================================================================================
    // INNER CLASSES
    // =================================================================================================================

    public static class DummyTrigger implements ChronoGraphPostCommitTrigger {

        public DummyTrigger() {
            // default constructor for kryo
        }

        @Override
        public void onPostCommit(final PostCommitTriggerContext context) {
            // does nothing
        }

        @Override
        public int getPriority() {
            return 0;
        }

    }

    public static class DummyTrigger2 implements ChronoGraphPostCommitTrigger {

        public DummyTrigger2() {
            // default constructor for kryo
        }

        @Override
        public void onPostCommit(final PostCommitTriggerContext context) {
            // does nothing
        }

        @Override
        public int getPriority() {
            return 0;
        }
    }

    public static class CallTrackingTrigger implements ChronoGraphPreCommitTrigger, ChronoGraphPrePersistTrigger, ChronoGraphPostPersistTrigger, ChronoGraphPostCommitTrigger {

        private final List<TriggerTiming> timings = Lists.newArrayList();

        public CallTrackingTrigger() {
        }

        @Override
        public void onPreCommit(final PreCommitTriggerContext context) throws CancelCommitException {
            this.timings.add(TriggerTiming.PRE_COMMIT);
        }

        @Override
        public void onPrePersist(final PrePersistTriggerContext context) throws CancelCommitException {
            this.timings.add(TriggerTiming.PRE_PERSIST);
        }

        @Override
        public void onPostPersist(final PostPersistTriggerContext context) {
            this.timings.add(TriggerTiming.POST_PERSIST);
        }

        @Override
        public void onPostCommit(final PostCommitTriggerContext context) {
            this.timings.add(TriggerTiming.POST_COMMIT);
        }

        @Override
        public int getPriority() {
            return 0;
        }

        public List<TriggerTiming> getTrackedTimings() {
            return Collections.unmodifiableList(this.timings);
        }

        public void clearTrackedTimings() {
            this.timings.clear();
        }

    }

    public static abstract class VertexCountTrigger implements ChronoGraphTrigger {

        private Set<TriggerTiming> timings;

        protected VertexCountTrigger() {
            // default constructor for kryo
        }

        public void trigger(final TriggerContext context) {
            ChronoGraph graph = context.getCurrentState().getGraph();
            long count = graph.traversal().V().count().next();
            graph.variables().set("vertexCount", (int) count);
        }

        @Override
        public int getPriority() {
            return 0;
        }

        public static class VertexCountPreCommitTrigger extends VertexCountTrigger implements ChronoGraphPreCommitTrigger {

            public VertexCountPreCommitTrigger(){
                // default constructor for kryo
            }

            @Override
            public void onPreCommit(final PreCommitTriggerContext context) throws CancelCommitException {
                this.trigger(context);
            }

        }

        public static class VertexCountPrePersistTrigger extends VertexCountTrigger implements ChronoGraphPrePersistTrigger {

            public VertexCountPrePersistTrigger(){
                // default constructor for kryo
            }

            @Override
            public void onPrePersist(final PrePersistTriggerContext context) throws CancelCommitException {
                this.trigger(context);
            }

        }

        public static class VertexCountPostPersistTrigger extends VertexCountTrigger implements ChronoGraphPostPersistTrigger {

            public VertexCountPostPersistTrigger(){
                // default constructor for kryo
            }

            @Override
            public void onPostPersist(final PostPersistTriggerContext context){
                this.trigger(context);
            }

        }

        public static class VertexCountPostCommitTrigger extends VertexCountTrigger implements ChronoGraphPostCommitTrigger {

            public VertexCountPostCommitTrigger(){
                // default constructor for kryo
            }

            @Override
            public void onPostCommit(final PostCommitTriggerContext context) {
                this.trigger(context);
            }

        }

    }

    public static abstract class ChangeSetRecordingTrigger implements ChronoGraphTrigger {

        private Set<String> observedModifiedVertexIds = null;
        private Set<String> observedModifiedEdgeIds = null;
        private Set<String> observedModifiedGraphVariableKeys = null;

        protected ChangeSetRecordingTrigger() {
            // default constructor for kryo
        }


        public void trigger(final TriggerContext context) {
            this.observedModifiedVertexIds = context.getCurrentState().getModifiedVertices().stream().map(ChronoVertex::id).collect(Collectors.toSet());
            this.observedModifiedEdgeIds = context.getCurrentState().getModifiedEdges().stream().map(ChronoEdge::id).collect(Collectors.toSet());
            this.observedModifiedGraphVariableKeys = Sets.newHashSet(context.getCurrentState().getModifiedGraphVariables());
        }

        @Override
        public int getPriority() {
            return 0;
        }

        public static class ChangeSetRecordingPostPersistTrigger extends ChangeSetRecordingTrigger implements ChronoGraphPostPersistTrigger {

            public ChangeSetRecordingPostPersistTrigger(){
                // default constructor for kryo
            }

            @Override
            public void onPostPersist(final PostPersistTriggerContext context) {
                super.trigger(context);
            }
        }

        public static class ChangeSetRecordingPostCommitTrigger extends ChangeSetRecordingTrigger implements ChronoGraphPostCommitTrigger {

            public ChangeSetRecordingPostCommitTrigger(){
                // default constructor for kryo
            }

            @Override
            public void onPostCommit(final PostCommitTriggerContext context) {
                super.trigger(context);
            }
        }
    }

    public static abstract class AppendPriorityTrigger implements ChronoGraphTrigger {

        private int priority;

        protected AppendPriorityTrigger() {
            // default constructor for kryo
        }

        protected AppendPriorityTrigger(int priority) {
            this.priority = priority;
        }


        public void trigger(final TriggerContext context) {
            ChronoGraph graph = context.getCurrentState().getGraph();
            Optional<Object> priority = graph.variables().get("priority");
            if (priority.isPresent()) {
                String value = (String) priority.get();
                value += "," + this.priority;
                graph.variables().set("priority", value);
            } else {
                graph.variables().set("priority", "" + this.priority);
            }
        }


        @Override
        public int getPriority() {
            return this.priority;
        }

        public static class AppendPriorityPreCommitTrigger extends AppendPriorityTrigger implements ChronoGraphPreCommitTrigger{

            protected AppendPriorityPreCommitTrigger() {
                // default constructor for kryo
            }

            public AppendPriorityPreCommitTrigger(int priority) {
                super(priority);
            }

            @Override
            public void onPreCommit(final PreCommitTriggerContext context) throws CancelCommitException {
                this.trigger(context);
            }

        }

        public static class AppendPriorityPrePersistTrigger extends AppendPriorityTrigger implements ChronoGraphPrePersistTrigger{

            protected AppendPriorityPrePersistTrigger() {
                // default constructor for kryo
            }

            public AppendPriorityPrePersistTrigger(int priority) {
                super(priority);
            }

            @Override
            public void onPrePersist(final PrePersistTriggerContext context) throws CancelCommitException {
                this.trigger(context);
            }

        }

        public static class AppendPriorityPostPersistTrigger extends AppendPriorityTrigger implements ChronoGraphPostPersistTrigger{

            protected AppendPriorityPostPersistTrigger() {
                // default constructor for kryo
            }

            public AppendPriorityPostPersistTrigger(int priority) {
                super(priority);
            }

            @Override
            public void onPostPersist(final PostPersistTriggerContext context) {
                this.trigger(context);
            }

        }

        public static class AppendPriorityPostCommitTrigger extends AppendPriorityTrigger implements ChronoGraphPostCommitTrigger{

            protected AppendPriorityPostCommitTrigger() {
                // default constructor for kryo
            }

            public AppendPriorityPostCommitTrigger(int priority) {
                super(priority);
            }

            @Override
            public void onPostCommit(final PostCommitTriggerContext context) {
                this.trigger(context);
            }

        }

    }

    public static class OnlyMasterBranchTrigger implements ChronoGraphPrePersistTrigger {

        public OnlyMasterBranchTrigger() {
            // default constructor for kryo
        }

        @Override
        public void onPrePersist(final PrePersistTriggerContext context) throws CancelCommitException {
            if (context.getBranch().getName().equals(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER)) {
                Variables variables = context.getCurrentState().getGraph().variables();
                Optional<Integer> count = variables.get("count");
                if (count.isPresent()) {
                    Integer value = count.get();
                    variables.set("count", value + 1);
                } else {
                    variables.set("count", 1);
                }
            }
        }

        @Override
        public int getPriority() {
            return 0;
        }

    }

    public static class RollbackTrigger implements ChronoGraphPrePersistTrigger {

        public RollbackTrigger() {
            // default constructor for kryo
        }

        @Override
        public void onPrePersist(final PrePersistTriggerContext context) throws CancelCommitException {
            // attempt a rollback (which is prohibited in triggers)
            context.getCurrentState().getGraph().tx().rollback();
        }

        @Override
        public int getPriority() {
            return 0;
        }

    }

    public static class OnDeleteTrigger implements ChronoGraphPostPersistTrigger {

        private Set<String> deletedVertexIds = Sets.newHashSet();

        public OnDeleteTrigger() {
            // default constructor for kryo
        }

        @Override
        public void onPostPersist(final PostPersistTriggerContext context) {
            Set<ChronoVertex> modifiedVertices = context.getCurrentState().getModifiedVertices();
            this.deletedVertexIds.addAll(modifiedVertices.stream().filter(ChronoVertex::isRemoved).filter(v -> v.getStatus() != ElementLifecycleStatus.OBSOLETE).map(ChronoVertex::id).collect(Collectors.toSet()));
        }

        @Override
        public int getPriority() {
            return 0;
        }

    }

    public static class FakeTrigger implements ChronoGraphTrigger{

        public FakeTrigger(){

        }

        @Override
        public int getPriority() {
            return 0;
        }

    }

    public static class AccessBranchAndTimestampTrigger implements ChronoGraphPreCommitTrigger, ChronoGraphPrePersistTrigger, ChronoGraphPostPersistTrigger, ChronoGraphPostCommitTrigger {

        private final List<Exception> exceptions = Lists.newArrayList();

        public AccessBranchAndTimestampTrigger(){
            // default constructor for kryo
        }

        private void accessBranchAndTimestamp(TriggerContext ctx){
            try{
                ctx.getCurrentState().getBranch();
                ctx.getCurrentState().getTimestamp();
                ctx.getAncestorState().getBranch();
                ctx.getAncestorState().getTimestamp();
                ctx.getStoreState().getBranch();
                ctx.getStoreState().getTimestamp();
            }catch(Exception e){
                this.exceptions.add(e);
            }
        }

        @Override
        public void onPreCommit(final PreCommitTriggerContext context) throws CancelCommitException {
           this.accessBranchAndTimestamp(context);
        }

        @Override
        public void onPrePersist(final PrePersistTriggerContext context) throws CancelCommitException {
            this.accessBranchAndTimestamp(context);
        }

        @Override
        public void onPostPersist(final PostPersistTriggerContext context) {
            this.accessBranchAndTimestamp(context);
        }


        @Override
        public void onPostCommit(final PostCommitTriggerContext context) {
            this.accessBranchAndTimestamp(context);
        }

        @Override
        public int getPriority() {
            return 0;
        }

        public List<Exception> getExceptions() {
            return exceptions;
        }
    }

    public static class PreCommitStoreStateChecker implements ChronoGraphPostPersistTrigger {

        public static List<String> RECORDED_VERTEX_IDS = Lists.newArrayList();

        public PreCommitStoreStateChecker(){
            // default constructor for kryo
        }

        @Override
        public void onPostPersist(final PostPersistTriggerContext context) {
            RECORDED_VERTEX_IDS.clear();
            ChronoGraph preCommitStoreState = context.getPreCommitStoreState().getGraph();
            List<Vertex> preCommitVertices = Lists.newArrayList(preCommitStoreState.vertices());
            preCommitVertices.forEach(v -> RECORDED_VERTEX_IDS.add((String) v.id()));
        }

        @Override
        public int getPriority() {
            return 0;
        }

    }


    public static class PostPersistDeletionFinder implements ChronoGraphPostPersistTrigger {

        public static List<String> DETECTED_DELETIONS = Lists.newArrayList();

        public PostPersistDeletionFinder(){
            // default constructor for kryo
        }

        @Override
        public void onPostPersist(final PostPersistTriggerContext context) {
            DETECTED_DELETIONS.clear();
            ChronoGraph preCommitGraph = context.getPreCommitStoreState().getGraph();
            List<String> detectedVertexDeletions = context.getCurrentState().getModifiedVertices().stream()
                // find the vertices which were removed by this transaction
                .filter(ChronoVertex::isRemoved)
                .map(ChronoVertex::id)
                // fetch them from the previous graph state
                // (imagine we wanted to access some property, which we can't do in
                // our current state since the vertex is removed)
                .map(preCommitGraph::vertex)
                .filter(Objects::nonNull)
                // for illustration purposes, just fetch the ID again
                .map(Vertex::id).map(Object::toString)
                .collect(Collectors.toList());
            DETECTED_DELETIONS.addAll(detectedVertexDeletions);
        }

        @Override
        public int getPriority() {
            return 0;
        }

    }

}
