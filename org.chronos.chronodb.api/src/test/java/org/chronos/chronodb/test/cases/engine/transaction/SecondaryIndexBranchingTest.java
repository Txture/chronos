package org.chronos.chronodb.test.cases.engine.transaction;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.cases.util.model.person.FirstNameIndexer;
import org.chronos.chronodb.test.cases.util.model.person.LastNameIndexer;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.chronos.common.test.utils.model.person.Person;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class SecondaryIndexBranchingTest extends AllChronoDBBackendsTest {

    @Test
    public void canQuerySecondaryIndexOnBranchBeforeBranchingTimestamp(){
        ChronoDB db = this.getChronoDB();
        db.getIndexManager().addIndexer("firstName", new FirstNameIndexer());
        db.getIndexManager().addIndexer("lastName", new LastNameIndexer());
        long afterFirstCommit;
        { // first insert
            ChronoDBTransaction tx = db.tx();
            tx.put("john.doe", new Person("John", "Doe"));
            tx.put("jane.doe", new Person("Jane", "Doe"));
            afterFirstCommit = tx.commit();
        }

        // create the branch
        db.getBranchManager().createBranch("my-branch");

        // on both master and my-branch, delete john

        {
            ChronoDBTransaction tx = db.tx();
            tx.remove("john.doe");
            tx.commit();
        }

        {
            ChronoDBTransaction tx = db.tx("my-branch");
            tx.remove("john.doe");
            tx.commit();
        }

        { // query on the branch for the first commit
            Set<QualifiedKey> johns = db.tx("my-branch", afterFirstCommit).find().inDefaultKeyspace().where("firstName").isEqualTo("John").getKeysAsSet();
            assertThat(johns, contains(QualifiedKey.createInDefaultKeyspace("john.doe")));
        }

        { // reindex and try again
            db.getIndexManager().reindexAll(true);
            Set<QualifiedKey> johns = db.tx("my-branch", afterFirstCommit).find().inDefaultKeyspace().where("firstName").isEqualTo("John").getKeysAsSet();
            assertThat(johns, contains(QualifiedKey.createInDefaultKeyspace("john.doe")));
        }
    }
}
