package org.chronos.chronograph.test.cases.transaction.trigger;

import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPostCommitTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPostPersistTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPreCommitTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPrePersistTrigger;
import org.chronos.chronograph.api.transaction.trigger.GraphTriggerMetadata;
import org.chronos.chronograph.api.transaction.trigger.ScriptedGraphTrigger;
import org.chronos.chronograph.internal.impl.transaction.trigger.script.ScriptedGraphPostCommitTrigger;
import org.chronos.chronograph.internal.impl.transaction.trigger.script.ScriptedGraphPostPersistTrigger;
import org.chronos.chronograph.internal.impl.transaction.trigger.script.ScriptedGraphPreCommitTrigger;
import org.chronos.chronograph.internal.impl.transaction.trigger.script.ScriptedGraphPrePersistTrigger;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class ChronoGraphScriptedTriggerTest extends AllChronoGraphBackendsTest {

    @Before
    public void prepareTracker() {
        ScriptedTriggerTestTracker.getInstance().clearMessages();
    }

    @After
    public void clearTracker() {
        ScriptedTriggerTestTracker.getInstance().clearMessages();
    }

    @Test
    public void canUseScriptedGraphTriggers() {
        ChronoGraph graph = this.getGraph();

        String trigger1Script = ScriptedTriggerTestTracker.class.getName() + ".getInstance().addMessage("
            + "\"PRE COMMIT: v=\" + context.getCurrentState().getModifiedVertices().size() + \", e=\" + context.getCurrentState().getModifiedEdges().size()"
            + ")";
        ChronoGraphPreCommitTrigger trigger1 = ScriptedGraphTrigger.createPreCommitTrigger(trigger1Script, 0);

        String trigger2Script = ScriptedTriggerTestTracker.class.getName() + ".getInstance().addMessage("
            + "\"PRE PERSIST: v=\" + context.getCurrentState().getModifiedVertices().size() + \", e=\" + context.getCurrentState().getModifiedEdges().size()"
            + ")";
        ChronoGraphPrePersistTrigger trigger2 = ScriptedGraphTrigger.createPrePersistTrigger(trigger2Script, 0);

        String trigger3Script = ScriptedTriggerTestTracker.class.getName() + ".getInstance().addMessage("
            + "\"POST PERSIST: v=\" + context.getCurrentState().getModifiedVertices().size() + \", e=\" + context.getCurrentState().getModifiedEdges().size()"
            + ")";
        ChronoGraphPostPersistTrigger trigger3 = ScriptedGraphTrigger.createPostPersistTrigger(trigger3Script, 0);

        String trigger4Script = ScriptedTriggerTestTracker.class.getName() + ".getInstance().addMessage("
            + "\"POST COMMIT: v=\" + context.getCurrentState().getModifiedVertices().size() + \", e=\" + context.getCurrentState().getModifiedEdges().size()"
            + ")";
        ChronoGraphPostCommitTrigger trigger4 = ScriptedGraphTrigger.createPostCommitTrigger(trigger4Script, 0);

        graph.getTriggerManager().createTrigger("trigger1", trigger1);
        graph.getTriggerManager().createTrigger("trigger2", trigger2);
        graph.getTriggerManager().createTrigger("trigger3", trigger3);
        graph.getTriggerManager().createTrigger("trigger4", trigger4);

        graph.tx().open();
        try{
            Vertex john = graph.addVertex(T.id, "john");
            Vertex jane = graph.addVertex(T.id, "jane");
            john.addEdge("marriedTo", jane);
            graph.tx().commit();
        }finally{
            if(graph.tx().isOpen()){
                graph.tx().rollback();
            }
        }

        // our triggers should have fired and reported to the (static) tracker.
        List<String> messages = ScriptedTriggerTestTracker.getInstance().getMessages();
        assertThat(messages.size(), is(4));

        assertThat(messages.get(0), is("PRE COMMIT: v=2, e=1"));
        assertThat(messages.get(1), is("PRE PERSIST: v=2, e=1"));
        assertThat(messages.get(2), is("POST PERSIST: v=2, e=1"));
        assertThat(messages.get(3), is("POST COMMIT: v=2, e=1"));

        // make sure that trigger metadata is accessible
        List<GraphTriggerMetadata> triggerMetadata = graph.getTriggerManager().getTriggers();
        assertThat(triggerMetadata.size(), is(4));
        assertThat(triggerMetadata.get(0).getTriggerName(), is("trigger1"));
        assertThat(triggerMetadata.get(0).getTriggerClassName(), is(ScriptedGraphPreCommitTrigger.class.getName()));
        assertThat(triggerMetadata.get(0).getPriority(), is(0));
        assertThat(triggerMetadata.get(0).getInstantiationException(), is(nullValue()));
        assertThat(triggerMetadata.get(0).getUserScriptContent(), is(trigger1Script));
        assertThat(triggerMetadata.get(0).isPreCommitTrigger(), is(true));
        assertThat(triggerMetadata.get(0).isPrePersistTrigger(), is(false));
        assertThat(triggerMetadata.get(0).isPostPersistTrigger(), is(false));
        assertThat(triggerMetadata.get(0).isPostCommitTrigger(), is(false));

        assertThat(triggerMetadata.get(1).getTriggerName(), is("trigger2"));
        assertThat(triggerMetadata.get(1).getTriggerClassName(), is(ScriptedGraphPrePersistTrigger.class.getName()));
        assertThat(triggerMetadata.get(1).getPriority(), is(0));
        assertThat(triggerMetadata.get(1).getInstantiationException(), is(nullValue()));
        assertThat(triggerMetadata.get(1).getUserScriptContent(), is(trigger2Script));
        assertThat(triggerMetadata.get(1).isPreCommitTrigger(), is(false));
        assertThat(triggerMetadata.get(1).isPrePersistTrigger(), is(true));
        assertThat(triggerMetadata.get(1).isPostPersistTrigger(), is(false));
        assertThat(triggerMetadata.get(1).isPostCommitTrigger(), is(false));

        assertThat(triggerMetadata.get(2).getTriggerName(), is("trigger3"));
        assertThat(triggerMetadata.get(2).getTriggerClassName(), is(ScriptedGraphPostPersistTrigger.class.getName()));
        assertThat(triggerMetadata.get(2).getPriority(), is(0));
        assertThat(triggerMetadata.get(2).getInstantiationException(), is(nullValue()));
        assertThat(triggerMetadata.get(2).getUserScriptContent(), is(trigger3Script));
        assertThat(triggerMetadata.get(2).isPreCommitTrigger(), is(false));
        assertThat(triggerMetadata.get(2).isPrePersistTrigger(), is(false));
        assertThat(triggerMetadata.get(2).isPostPersistTrigger(), is(true));
        assertThat(triggerMetadata.get(2).isPostCommitTrigger(), is(false));

        assertThat(triggerMetadata.get(3).getTriggerName(), is("trigger4"));
        assertThat(triggerMetadata.get(3).getTriggerClassName(), is(ScriptedGraphPostCommitTrigger.class.getName()));
        assertThat(triggerMetadata.get(3).getPriority(), is(0));
        assertThat(triggerMetadata.get(3).getInstantiationException(), is(nullValue()));
        assertThat(triggerMetadata.get(3).getUserScriptContent(), is(trigger4Script));
        assertThat(triggerMetadata.get(3).isPreCommitTrigger(), is(false));
        assertThat(triggerMetadata.get(3).isPrePersistTrigger(), is(false));
        assertThat(triggerMetadata.get(3).isPostPersistTrigger(), is(false));
        assertThat(triggerMetadata.get(3).isPostCommitTrigger(), is(true));
    }


    public static class ScriptedTriggerTestTracker {

        private static final ScriptedTriggerTestTracker INSTANCE = new ScriptedTriggerTestTracker();

        public static ScriptedTriggerTestTracker getInstance() {
            return INSTANCE;
        }

        private final List<String> messages = Lists.newArrayList();

        public void addMessage(String message) {
            messages.add(message);
        }

        public List<String> getMessages() {
            return Collections.unmodifiableList(this.messages);
        }

        public void clearMessages() {
            this.messages.clear();
        }

    }

}
