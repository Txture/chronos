package org.chronos.chronograph.api.builder.query;

import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.function.BiPredicate;

import static com.google.common.base.Preconditions.*;

public class StringWithoutCP implements BiPredicate<Object, Collection> {

    private final TextMatchMode matchMode;

    public StringWithoutCP(TextMatchMode matchMode) {
        checkNotNull(matchMode, "Precondition violation - argument 'matchMode' must not be NULL!");
        this.matchMode = matchMode;
    }

    public TextMatchMode getMatchMode() {
        return this.matchMode;
    }

    @Override
    public boolean test(final Object o, final Collection collection) {
        return !this.negate().test(o, collection);
    }

    @NotNull
    @Override
    public BiPredicate<Object, Collection> negate() {
        return new StringWithinCP(this.matchMode);
    }

    @Override
    public String toString() {
        return "String Without" + (this.matchMode == TextMatchMode.CASE_INSENSITIVE ? " [CI]" : "");
    }

}
