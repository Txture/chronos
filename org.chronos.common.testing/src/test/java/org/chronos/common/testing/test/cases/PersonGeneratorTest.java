package org.chronos.common.testing.test.cases;

import org.chronos.common.test.utils.model.person.Person;
import org.chronos.common.test.utils.model.person.PersonGenerator;
import org.junit.Test;

import static org.junit.Assert.*;

public class PersonGeneratorTest {

    @Test
    public void canGeneratePerson(){
        Person person = PersonGenerator.generateRandomPerson();
        assertNotNull(person);
        assertNotNull(person.getFirstName());
        assertNotNull(person.getLastName());
        assertTrue(person.getFirstName().length() > 0);
        assertTrue(person.getLastName().length() > 0);
    }

}
