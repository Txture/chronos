package org.chronos.chronodb.internal.impl.dump.meta;

import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.impl.index.SecondaryIndexImpl;
import org.chronos.common.serialization.KryoManager;

import java.util.Base64;
import java.util.Collections;

public class IndexerDumpMetadataV2 implements IIndexerDumpMetadata {

    private String id;
    private String indexName;
    private String indexerData;
    private String branch;
    private Long validFrom;
    private Long validTo;
    private String parentIndexId;


    protected IndexerDumpMetadataV2() {
    }

    public IndexerDumpMetadataV2(SecondaryIndex index) {
        this.id = index.getId();
        this.indexName = index.getName();
        byte[] indexerSerialized = KryoManager.serialize(index.getIndexer());
        this.indexerData = Base64.getEncoder().encodeToString(indexerSerialized);
        this.branch = index.getBranch();
        this.validFrom = index.getValidPeriod().getLowerBound();
        this.validTo = index.getValidPeriod().getUpperBound();
        this.parentIndexId = index.getParentIndexId();
    }

    public SecondaryIndex toSecondaryIndex() {
        byte[] serialForm = Base64.getDecoder().decode(this.indexerData);
        Indexer<?> indexer = KryoManager.deserialize(serialForm);
        return new SecondaryIndexImpl(
            id,
            indexName,
            indexer,
            Period.createRange(validFrom, validTo),
            branch,
            parentIndexId,
            true,
            Collections.emptySet()
        );
    }
}
