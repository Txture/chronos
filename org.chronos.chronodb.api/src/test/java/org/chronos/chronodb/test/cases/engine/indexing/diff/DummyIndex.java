package org.chronos.chronodb.test.cases.engine.indexing.diff;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.impl.index.IndexingOption;
import org.chronos.chronodb.internal.impl.index.diff.IndexingUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public class DummyIndex implements SecondaryIndex {

        private final String id = UUID.randomUUID().toString();

        private final String name;

        private final Indexer<?> indexer;

        public DummyIndex(Indexer<?> indexer) {
            this(indexer, null);
        }

        public DummyIndex(Indexer<?> indexer, String name){
            checkNotNull(indexer, "Precondition violation - argument 'indexer' must not be NULL!");
            this.indexer = indexer;
            if(name == null){
                this.name = "Dummy-" + this.id;
            }else{
                this.name = name;
            }
        }

        @NotNull
        @Override
        public String getId() {
            return this.id;
        }

        @NotNull
        @Override
        public String getName() {
            return this.name;
        }

        @NotNull
        @Override
        public Indexer<?> getIndexer() {
            return this.indexer;
        }

        @NotNull
        @Override
        public Period getValidPeriod() {
            return Period.eternal();
        }

        @NotNull
        @Override
        public String getBranch() {
            return ChronoDBConstants.MASTER_BRANCH_IDENTIFIER;
        }

        @Nullable
        @Override
        public String getParentIndexId() {
            return null;
        }

        @Override
        public boolean getDirty() {
            return false;
        }

        @NotNull
        @Override
        public Class<?> getValueType() {
            return String.class;
        }

        @NotNull
        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Set<Comparable<?>> getIndexedValuesForObject(@Nullable final Object value) {
            if (value == null) {
                // when the object to index is null, we can't produce any indexed values.
                // empty set is already unmodifiable, no need to wrap it in Collections.umodifiableSet.
                return Collections.emptySet();
            }
            if (!this.indexer.canIndex(value)) {
                return Collections.emptySet();
            }
            Set<?> indexedValues = this.indexer.getIndexValues(value);
            if (indexedValues == null) {
                return Collections.emptySet();
            }
            // make sure that there are no NULL values or empty values in the indexed values set
            return (Set)indexedValues.stream()
                .filter(IndexingUtils::isValidIndexValue)
                .collect(Collectors.toSet());
        }

    @NotNull
    @Override
    public Set<IndexingOption> getOptions() {
        return Collections.emptySet();
    }

    @NotNull
    @Override
    public Set<IndexingOption> getInheritableOptions() {
        return Collections.emptySet();
    }
}