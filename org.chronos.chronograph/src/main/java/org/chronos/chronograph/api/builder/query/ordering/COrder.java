package org.chronos.chronograph.api.builder.query.ordering;

import org.chronos.chronodb.api.NullSortPosition;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.api.TextCompare;

import java.util.Comparator;

import static com.google.common.base.Preconditions.*;

/**
 * ChronoGraph-specific variant of {@link org.apache.tinkerpop.gremlin.process.traversal.Order} that allows for more configuration options.
 *
 */
public interface COrder extends Comparator<Object> {

    public static COrder asc() {
        return asc(TextCompare.DEFAULT, NullSortPosition.DEFAULT);
    }

    public static COrder asc(TextCompare textCompare){
        checkNotNull(textCompare, "Precondition violation - argument 'textCompare' must not be NULL!");
        return asc(textCompare, NullSortPosition.DEFAULT);
    }

    public static COrder asc(NullSortPosition nullSortPosition) {
        checkNotNull(nullSortPosition, "Precondition violation - argument 'nullSortPosition' must not be NULL!");
        return asc(TextCompare.DEFAULT, nullSortPosition);
    }

    public static COrder asc(TextCompare textCompare, NullSortPosition nullSortPosition){
        checkNotNull(textCompare, "Precondition violation - argument 'textCompare' must not be NULL!");
        checkNotNull(nullSortPosition, "Precondition violation - argument 'null' must not be NULL!");
        return new AscendingCOrder(textCompare, nullSortPosition);
    }

    public static COrder desc() {
        return desc(TextCompare.DEFAULT, NullSortPosition.DEFAULT);
    }

    public static COrder desc(TextCompare textCompare){
        checkNotNull(textCompare, "Precondition violation - argument 'textCompare' must not be NULL!");
        return desc(textCompare, NullSortPosition.DEFAULT);
    }

    public static COrder desc(NullSortPosition nullSortPosition) {
        checkNotNull(nullSortPosition, "Precondition violation - argument 'nullSortPosition' must not be NULL!");
        return desc(TextCompare.DEFAULT, nullSortPosition);
    }

    public static COrder desc(TextCompare textCompare, NullSortPosition nullSortPosition){
        checkNotNull(textCompare, "Precondition violation - argument 'textCompare' must not be NULL!");
        checkNotNull(nullSortPosition, "Precondition violation - argument 'null' must not be NULL!");
        return new DescendingCOrder(textCompare, nullSortPosition);
    }

    public Object normalize(Object obj);

    public Order getDirection();

    public TextCompare getTextCompare();

    public NullSortPosition getNullSortPosition();

    public COrder reversed();

    public int compare(Object first, Object second);

}
