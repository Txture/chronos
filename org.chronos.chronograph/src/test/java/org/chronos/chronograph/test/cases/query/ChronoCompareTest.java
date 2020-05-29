package org.chronos.chronograph.test.cases.query;

import org.chronos.chronograph.internal.impl.query.ChronoStringCompare;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class ChronoCompareTest {

    @Test
    public void testChronoCompareNegation(){
        for(ChronoStringCompare compare : ChronoStringCompare.values()){
            assertThat(compare.negate().negate(), is(compare));
        }
    }

}