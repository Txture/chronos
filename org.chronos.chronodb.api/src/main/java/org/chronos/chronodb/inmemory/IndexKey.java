package org.chronos.chronodb.inmemory;

import org.jetbrains.annotations.NotNull;

import static com.google.common.base.Preconditions.*;

public class IndexKey implements Comparable<IndexKey> {

    private final Comparable<?> indexValue;
    private final String key;

    public IndexKey(@NotNull Comparable<?> indexValue, @NotNull String key) {
        checkNotNull(indexValue, "Precondition violation - argument 'indexValue' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        this.indexValue = indexValue;
        this.key = key;
    }

    public Comparable<?> getIndexValue() {
        return indexValue;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "IndexKey{indexValue=" + indexValue + ", key='" + key + "'}";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        IndexKey indexKey = (IndexKey) o;

        if (!indexValue.equals(indexKey.indexValue))
            return false;
        return key.equals(indexKey.key);
    }

    @Override
    public int hashCode() {
        int result = indexValue.hashCode();
        result = 31 * result + key.hashCode();
        return result;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public int compareTo(@NotNull final IndexKey o) {
        int indexValueComparison = ((Comparable)this.indexValue).compareTo(o.getIndexValue());
        if(indexValueComparison != 0){
            return indexValueComparison;
        }
        return this.key.compareTo(o.getKey());
    }
}
