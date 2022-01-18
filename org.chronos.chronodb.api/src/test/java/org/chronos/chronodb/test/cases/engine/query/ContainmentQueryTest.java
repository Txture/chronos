package org.chronos.chronodb.test.cases.engine.query;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.indexing.DoubleIndexer;
import org.chronos.chronodb.api.indexing.LongIndexer;
import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.query.DoubleContainmentCondition;
import org.chronos.chronodb.api.query.LongContainmentCondition;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronodb.internal.impl.query.optimizer.StandardQueryOptimizer;
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryOperatorElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryQueryOperator;
import org.chronos.chronodb.internal.impl.query.parser.ast.QueryElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.SetDoubleWhereElement;
import org.chronos.chronodb.internal.impl.query.parser.ast.SetLongWhereElement;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.cases.util.model.payload.NamedPayloadNameIndexer;
import org.chronos.chronodb.test.cases.util.model.person.FirstNameIndexer;
import org.chronos.chronodb.test.cases.util.model.person.LastNameIndexer;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.chronos.common.test.utils.NamedPayload;
import org.chronos.common.test.utils.model.person.Person;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class ContainmentQueryTest extends AllChronoDBBackendsTest {

    @Test
    public void stringWithinQueryWorks() {
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

        Set<QualifiedKey> keysAsSet = db.tx().find()
            .inDefaultKeyspace()
            .where("name").inStrings(Sets.newHashSet("Foo Bar", "Foo Baz"))
            .getKeysAsSet();

        assertThat(keysAsSet, containsInAnyOrder(
                QualifiedKey.createInDefaultKeyspace("np2"),
                QualifiedKey.createInDefaultKeyspace("np3")
            )
        );
    }

    @Test
    public void stringWithoutQueryWorks() {
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

        Set<QualifiedKey> keysAsSet = db.tx().find()
            .inDefaultKeyspace()
            .where("name").notInStrings(Sets.newHashSet("Foo Bar", "Foo Baz"))
            .getKeysAsSet();

        assertThat(keysAsSet, containsInAnyOrder(QualifiedKey.createInDefaultKeyspace("np1")));
    }

    @Test
    public void longWithinQueryWorks() {
        ChronoDB db = this.getChronoDB();
        LongIndexer indexer = new LongBeanIndexer();
         db.getIndexManager().createIndex().withName("value").withIndexer(indexer).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();

        ChronoDBTransaction tx = db.tx();
        tx.put("v1", new LongBean(1L));
        tx.put("v2", new LongBean(2L));
        tx.put("v3", new LongBean(3L));
        tx.put("v4", new LongBean(4L));
        tx.put("v5", new LongBean(5L));
        tx.commit();


        Set<QualifiedKey> keysAsSet = db.tx().find()
            .inDefaultKeyspace()
            .where("value").inLongs(Sets.newHashSet(2L, 4L, 5L))
            .getKeysAsSet();

        assertThat(keysAsSet, containsInAnyOrder(
            QualifiedKey.createInDefaultKeyspace("v2"),
            QualifiedKey.createInDefaultKeyspace("v4"),
            QualifiedKey.createInDefaultKeyspace("v5")
        ));
    }

    @Test
    public void longWithoutQueryWorks() {
        ChronoDB db = this.getChronoDB();
        LongIndexer indexer = new LongBeanIndexer();
         db.getIndexManager().createIndex().withName("value").withIndexer(indexer).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();

        ChronoDBTransaction tx = db.tx();
        tx.put("v1", new LongBean(1L));
        tx.put("v2", new LongBean(2L));
        tx.put("v3", new LongBean(3L));
        tx.put("v4", new LongBean(4L));
        tx.put("v5", new LongBean(5L));
        tx.commit();


        Set<QualifiedKey> keysAsSet = db.tx().find()
            .inDefaultKeyspace()
            .where("value").notInLongs(Sets.newHashSet(2L, 4L, 5L))
            .getKeysAsSet();

        assertThat(keysAsSet, containsInAnyOrder(
            QualifiedKey.createInDefaultKeyspace("v1"),
            QualifiedKey.createInDefaultKeyspace("v3")
        ));

    }

    @Test
    public void doubleWithinQueryWorks() {
        ChronoDB db = this.getChronoDB();
        DoubleIndexer indexer = new DoubleBeanIndexer();
         db.getIndexManager().createIndex().withName("value").withIndexer(indexer).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();

        ChronoDBTransaction tx = db.tx();
        tx.put("v1", new DoubleBean(1.3));
        tx.put("v2", new DoubleBean(2.3));
        tx.put("v3", new DoubleBean(3.3));
        tx.put("v4", new DoubleBean(4.3));
        tx.put("v5", new DoubleBean(5.3));
        tx.commit();


        Set<QualifiedKey> keysAsSet = db.tx().find()
            .inDefaultKeyspace()
            .where("value").inDoubles(Sets.newHashSet(2.0, 4.0, 5.0), 0.4)
            .getKeysAsSet();

        assertThat(keysAsSet, containsInAnyOrder(
            QualifiedKey.createInDefaultKeyspace("v2"),
            QualifiedKey.createInDefaultKeyspace("v4"),
            QualifiedKey.createInDefaultKeyspace("v5")
        ));
    }


    @Test
    public void doubleWithoutQueryWorks() {
        ChronoDB db = this.getChronoDB();
        DoubleIndexer indexer = new DoubleBeanIndexer();
         db.getIndexManager().createIndex().withName("value").withIndexer(indexer).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();

        ChronoDBTransaction tx = db.tx();
        tx.put("v1", new DoubleBean(1.3));
        tx.put("v2", new DoubleBean(2.3));
        tx.put("v3", new DoubleBean(3.3));
        tx.put("v4", new DoubleBean(4.3));
        tx.put("v5", new DoubleBean(5.3));
        tx.commit();


        Set<QualifiedKey> keysAsSet = db.tx().find()
            .inDefaultKeyspace()
            .where("value").notInDoubles(Sets.newHashSet(2.0, 4.0, 5.0), 0.4)
            .getKeysAsSet();

        assertThat(keysAsSet, containsInAnyOrder(
            QualifiedKey.createInDefaultKeyspace("v1"),
            QualifiedKey.createInDefaultKeyspace("v3")
        ));
    }

    @Test
    public void withinAndConnectionQueryWorks() {
        ChronoDB db = this.getChronoDB();
        db.getIndexManager().createIndex().withName("first name").withIndexer(new FirstNameIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().createIndex().withName("last name").withIndexer(new LastNameIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();

        ChronoDBTransaction tx = db.tx();
        tx.put("p1", new Person("John", "Doe"));
        tx.put("p2", new Person("Jane", "Doe"));
        tx.put("p3", new Person("John", "Sparrow"));
        tx.put("p4", new Person("John", "Skywalker"));
        tx.put("p5", new Person("Foo", "Bar"));
        tx.commit();


        Set<QualifiedKey> keysAsSet = db.tx().find()
            .inDefaultKeyspace()
            .where("first name").inStrings(Sets.newHashSet("John", "Jane"))
            .and()
            .where("last name").notInStringsIgnoreCase(Sets.newHashSet("skywalker", "sparrow"))
            .getKeysAsSet();

        assertThat(keysAsSet, containsInAnyOrder(
            QualifiedKey.createInDefaultKeyspace("p1"),
            QualifiedKey.createInDefaultKeyspace("p2")
        ));
    }

    @Test
    public void canConvertLongOrQueriesIntoContainmentQuery() {
        ChronoDB db = this.getChronoDB();
        db.getIndexManager().createIndex().withName("x").withIndexer(new LongBeanIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().createIndex().withName("y").withIndexer(new LongBeanIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().createIndex().withName("z").withIndexer(new LongBeanIndexer()).onMaster().acrossAllTimestamps().build();

        { // within: merge where clauses with same index
            ChronoDBQuery query = db.tx().find().inDefaultKeyspace().where("x").isEqualTo(1).or().where("x").isEqualTo(2).or().where("x").isEqualTo(3).toQuery();
            ChronoDBQuery optimedQuery = new StandardQueryOptimizer().optimize(query);
            QueryElement rootElement = optimedQuery.getRootElement();
            assertThat(rootElement, is(instanceOf(SetLongWhereElement.class)));
            SetLongWhereElement inClause = (SetLongWhereElement) rootElement;
            assertThat(inClause.getCondition(), is(LongContainmentCondition.WITHIN));
            assertThat(inClause.getIndexName(), is("x"));
            assertThat(inClause.getComparisonValue(), containsInAnyOrder(1L, 2L, 3L));
        }

        { // within: do not merge where clauses with different indices
            ChronoDBQuery query = db.tx().find().inDefaultKeyspace().where("x").isEqualTo(1).or().where("y").isEqualTo(2).or().where("z").isEqualTo(3).toQuery();
            ChronoDBQuery optimedQuery = new StandardQueryOptimizer().optimize(query);
            QueryElement rootElement = optimedQuery.getRootElement();
            assertThat(rootElement, is((instanceOf(BinaryOperatorElement.class))));
        }


        { // without: merge where clauses with same index
            ChronoDBQuery query = db.tx().find().inDefaultKeyspace()
                .not().begin()
                .where("x").isEqualTo(1)
                .and().where("x").isEqualTo(2)
                .and().where("x").isEqualTo(3)
                .end()
                .toQuery();
            ChronoDBQuery optimedQuery = new StandardQueryOptimizer().optimize(query);
            QueryElement rootElement = optimedQuery.getRootElement();
            assertThat(rootElement, is(instanceOf(SetLongWhereElement.class)));
            SetLongWhereElement inClause = (SetLongWhereElement) rootElement;
            assertThat(inClause.getCondition(), is(LongContainmentCondition.WITHOUT));
            assertThat(inClause.getIndexName(), is("x"));
            assertThat(inClause.getComparisonValue(), containsInAnyOrder(1L, 2L, 3L));
        }

        { // without: merge where clauses with same index
            ChronoDBQuery query = db.tx().find().inDefaultKeyspace()
                .not().begin()
                .where("x").isEqualTo(1)
                .and().where("y").isEqualTo(2)
                .and().where("z").isEqualTo(3)
                .end()
                .toQuery();
            ChronoDBQuery optimedQuery = new StandardQueryOptimizer().optimize(query);
            QueryElement rootElement = optimedQuery.getRootElement();
            assertThat(rootElement, is((instanceOf(BinaryOperatorElement.class))));
        }

    }


    @Test
    public void canConvertNestedLongOrQueriesIntoContainmentQuery() {
        ChronoDB db = this.getChronoDB();
        db.getIndexManager().createIndex().withName("x").withIndexer(new LongBeanIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().createIndex().withName("y").withIndexer(new LongBeanIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().createIndex().withName("z").withIndexer(new LongBeanIndexer()).onMaster().acrossAllTimestamps().build();


        ChronoDBQuery query = db.tx().find().inDefaultKeyspace()
            .where("x").isEqualTo(1)
            .and().begin()
            .where("y").isEqualTo(2).or().where("y").isEqualTo(3).or().where("y").isEqualTo(4)
            .end()
            .toQuery();
        ChronoDBQuery optimedQuery = new StandardQueryOptimizer().optimize(query);
        QueryElement rootElement = optimedQuery.getRootElement();
        assertThat(rootElement, is(instanceOf(BinaryOperatorElement.class)));
        BinaryOperatorElement rootBinary = (BinaryOperatorElement) rootElement;
        assertThat(rootBinary.getOperator(), is(BinaryQueryOperator.AND));
        QueryElement rightChild = rootBinary.getRightChild();
        assertThat(rightChild, is(instanceOf(SetLongWhereElement.class)));

        SetLongWhereElement inClause = (SetLongWhereElement) rightChild;
        assertThat(inClause.getCondition(), is(LongContainmentCondition.WITHIN));
        assertThat(inClause.getIndexName(), is("y"));
        assertThat(inClause.getComparisonValue(), containsInAnyOrder(2L, 3L, 4L));
    }

    @Test
    public void canConvertDoubleOrQueriesIntoContainmentQuery() {
        ChronoDB db = this.getChronoDB();
        db.getIndexManager().createIndex().withName("x").withIndexer(new DoubleBeanIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().createIndex().withName("y").withIndexer(new DoubleBeanIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().createIndex().withName("z").withIndexer(new DoubleBeanIndexer()).onMaster().acrossAllTimestamps().build();

        { // within: merge where clauses with same index
            ChronoDBQuery query = db.tx().find().inDefaultKeyspace().where("x").isEqualTo(1.0, 0.01).or().where("x").isEqualTo(2.0, 0.01).or().where("x").isEqualTo(3.0, 0.01).toQuery();
            ChronoDBQuery optimedQuery = new StandardQueryOptimizer().optimize(query);
            QueryElement rootElement = optimedQuery.getRootElement();
            assertThat(rootElement, is(instanceOf(SetDoubleWhereElement.class)));
            SetDoubleWhereElement inClause = (SetDoubleWhereElement) rootElement;
            assertThat(inClause.getCondition(), is(DoubleContainmentCondition.WITHIN));
            assertThat(inClause.getIndexName(), is("x"));
            assertThat(inClause.getComparisonValue(), containsInAnyOrder(1.0, 2.0, 3.0));
            assertThat(inClause.getEqualityTolerance(), is(0.01));
        }

        { // within: do not merge where clauses with different indices
            ChronoDBQuery query = db.tx().find().inDefaultKeyspace().where("x").isEqualTo(1.0, 0.01).or().where("y").isEqualTo(2.0, 0.01).or().where("z").isEqualTo(3.0, 0.01).toQuery();
            ChronoDBQuery optimedQuery = new StandardQueryOptimizer().optimize(query);
            QueryElement rootElement = optimedQuery.getRootElement();
            assertThat(rootElement, is((instanceOf(BinaryOperatorElement.class))));
        }

        { // within: do not merge where clauses with different tolerances
            ChronoDBQuery query = db.tx().find().inDefaultKeyspace().where("x").isEqualTo(1.0, 0.01).or().where("x").isEqualTo(2.0, 0.02).or().where("x").isEqualTo(3.0, 0.03).toQuery();
            ChronoDBQuery optimedQuery = new StandardQueryOptimizer().optimize(query);
            QueryElement rootElement = optimedQuery.getRootElement();
            assertThat(rootElement, is((instanceOf(BinaryOperatorElement.class))));
        }


        { // without: merge where clauses with same index
            ChronoDBQuery query = db.tx().find().inDefaultKeyspace()
                .not().begin()
                .where("x").isEqualTo(1.0, 0.01)
                .and().where("x").isEqualTo(2.0, 0.01)
                .and().where("x").isEqualTo(3, 0.01)
                .end()
                .toQuery();
            ChronoDBQuery optimedQuery = new StandardQueryOptimizer().optimize(query);
            QueryElement rootElement = optimedQuery.getRootElement();
            assertThat(rootElement, is(instanceOf(SetDoubleWhereElement.class)));
            SetDoubleWhereElement inClause = (SetDoubleWhereElement) rootElement;
            assertThat(inClause.getCondition(), is(DoubleContainmentCondition.WITHOUT));
            assertThat(inClause.getIndexName(), is("x"));
            assertThat(inClause.getComparisonValue(), containsInAnyOrder(1.0, 2.0, 3.0));
            assertThat(inClause.getEqualityTolerance(), is(0.01));
        }

        { // without: merge where clauses with same index
            ChronoDBQuery query = db.tx().find().inDefaultKeyspace()
                .not().begin()
                .where("x").isEqualTo(1.0, 0.01)
                .and().where("y").isEqualTo(2.0, 0.01)
                .and().where("z").isEqualTo(3.0, 0.01)
                .end()
                .toQuery();
            ChronoDBQuery optimedQuery = new StandardQueryOptimizer().optimize(query);
            QueryElement rootElement = optimedQuery.getRootElement();
            assertThat(rootElement, is((instanceOf(BinaryOperatorElement.class))));
        }

        { // without: do not merge where clauses with different tolerances
            ChronoDBQuery query = db.tx().find().inDefaultKeyspace()
                .not().begin()
                .where("x").isEqualTo(1.0, 0.01)
                .and().where("x").isEqualTo(2.0, 0.02)
                .and().where("x").isEqualTo(3.0, 0.03)
                .end().toQuery();
            ChronoDBQuery optimedQuery = new StandardQueryOptimizer().optimize(query);
            QueryElement rootElement = optimedQuery.getRootElement();
            assertThat(rootElement, is((instanceOf(BinaryOperatorElement.class))));
        }
    }


    @Test
    public void canConvertNestedDoubleOrQueriesIntoContainmentQuery() {
        ChronoDB db = this.getChronoDB();
        db.getIndexManager().createIndex().withName("x").withIndexer(new DoubleBeanIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().createIndex().withName("y").withIndexer(new DoubleBeanIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().createIndex().withName("z").withIndexer(new DoubleBeanIndexer()).onMaster().acrossAllTimestamps().build();

        ChronoDBQuery query = db.tx().find().inDefaultKeyspace()
            .where("x").isEqualTo(1.0, 0.01)
            .and().begin()
            .where("y").isEqualTo(2.0, 0.01).or().where("y").isEqualTo(3.0, 0.01).or().where("y").isEqualTo(4.0, 0.01)
            .end()
            .toQuery();
        ChronoDBQuery optimedQuery = new StandardQueryOptimizer().optimize(query);
        QueryElement rootElement = optimedQuery.getRootElement();
        assertThat(rootElement, is(instanceOf(BinaryOperatorElement.class)));
        BinaryOperatorElement rootBinary = (BinaryOperatorElement) rootElement;
        assertThat(rootBinary.getOperator(), is(BinaryQueryOperator.AND));
        QueryElement rightChild = rootBinary.getRightChild();
        assertThat(rightChild, is(instanceOf(SetDoubleWhereElement.class)));

        SetDoubleWhereElement inClause = (SetDoubleWhereElement) rightChild;
        assertThat(inClause.getCondition(), is(DoubleContainmentCondition.WITHIN));
        assertThat(inClause.getIndexName(), is("y"));
        assertThat(inClause.getComparisonValue(), containsInAnyOrder(2.0, 3.0, 4.0));
    }

    private static class DoubleBean {

        private double value;

        @SuppressWarnings("unused")
        protected DoubleBean() {
            // kryo
        }

        public DoubleBean(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }
    }

    private static class DoubleBeanIndexer implements DoubleIndexer {

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
            return object instanceof DoubleBean;
        }

        @Override
        public Set<Double> getIndexValues(final Object object) {
            return Collections.singleton(((DoubleBean) object).getValue());
        }
    }

    private static class LongBean {

        private long value;

        @SuppressWarnings("unused")
        protected LongBean() {
            // kryo
        }

        public LongBean(long value) {
            this.value = value;
        }

        public long getValue() {
            return value;
        }
    }

    private static class LongBeanIndexer implements LongIndexer {

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
            return object instanceof LongBean;
        }

        @Override
        public Set<Long> getIndexValues(final Object object) {
            return Collections.singleton(((LongBean) object).getValue());
        }
    }

}
