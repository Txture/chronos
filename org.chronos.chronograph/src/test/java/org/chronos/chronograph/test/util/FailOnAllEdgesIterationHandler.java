package org.chronos.chronograph.test.util;

import org.chronos.chronograph.api.transaction.AllEdgesIterationHandler;

import static org.junit.Assert.*;

public class FailOnAllEdgesIterationHandler implements AllEdgesIterationHandler {

    @Override
    public void onAllEdgesIteration() {
        fail("Required iteration over all Edges!");
    }

}
