package org.chronos.chronograph.internal.impl.iterators.builder.states;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.chronos.chronograph.api.iterators.states.AllElementsState;
import org.chronos.chronograph.api.structure.ChronoGraph;

import static com.google.common.base.Preconditions.*;

public class AllElementsStateImpl extends GraphIteratorStateImpl implements AllElementsState {

    private final Element element;

    public AllElementsStateImpl(final ChronoGraph txGraph, Element element) {
        super(txGraph);
        checkNotNull(element, "Precondition violation - argument 'element' must not be NULL!");
        this.element = element;
    }


    @Override
    public Element getCurrentElement() {
        return this.element;
    }
}
