package org.chronos.chronodb.exodus.secondaryindex;

import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.impl.index.IndexingOption;
import org.chronos.chronodb.internal.impl.index.SecondaryIndexImpl;
import org.chronos.common.annotation.PersistentClass;
import org.chronos.common.serialization.KryoManager;

import java.util.Base64;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

@PersistentClass("kryo")
public class ExodusSecondaryIndex {

    private String id;
    private String name;
    private String indexerBytesBase64;
    private long validFrom;
    private long validTo;
    private String branch;
    private String parentIndexId;
    private Boolean dirty;
    private Set<IndexingOption> options;

    private ExodusSecondaryIndex() {
        // default constructor for deserialization
    }

    public ExodusSecondaryIndex(SecondaryIndex index) {
        checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
        this.id = index.getId();
        this.name = index.getName();
        byte[] indexerSerialized = KryoManager.serialize(index.getIndexer());
        this.indexerBytesBase64 = Base64.getEncoder().encodeToString(indexerSerialized);
        this.validFrom = index.getValidPeriod().getLowerBound();
        this.validTo = index.getValidPeriod().getUpperBound();
        this.branch = index.getBranch();
        this.parentIndexId = index.getParentIndexId();
        this.dirty = index.getDirty();
        this.options = index.getOptions();
    }

    public SecondaryIndex toSecondaryIndex() {
        byte[] serialForm = Base64.getDecoder().decode(this.indexerBytesBase64);
        Indexer<?> indexer = KryoManager.deserialize(serialForm);
        return new SecondaryIndexImpl(
            id,
            name,
            indexer,
            Period.createRange(validFrom, validTo),
            branch,
            parentIndexId,
            dirty,
            options
        );
    }
}
