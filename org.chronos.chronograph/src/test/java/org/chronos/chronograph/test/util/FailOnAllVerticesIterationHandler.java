package org.chronos.chronograph.test.util;

import org.chronos.chronograph.api.transaction.AllVerticesIterationHandler;

import static org.junit.Assert.*;

public class FailOnAllVerticesIterationHandler implements AllVerticesIterationHandler {

    @Override
    public void onAllVerticesIteration() {
        fail("Required iteration over all Vertices which is not supposed to happen in this test!");
    }

}
