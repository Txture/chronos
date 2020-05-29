package org.chronos.chronodb.api;

/**
 * A simple filter interface for determining whether or not a given commit metadata object is valid.
 *
 * <p>
 * Clients are encouraged to provide their own implementation. An active instance of this interface will
 * always be created using reflection and therefore requires a default constructor.
 * </p>
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface CommitMetadataFilter {

    /**
     * Checks if this filter permits the given commit metadata object.
     *
     * @param branch The branch at which the commit is aimed. Must not be <code>null</code>.
     * @param timestamp The intended timestamp of the commit. Never negative.
     * @param metadata The commit metadata object. May be <code>null</code>.
     * @return <code>true</code> if the filter accepts the given object as valid, or <code>false</code> if the given object does not pass the filter.
     */
    public boolean doesAccept(String branch, long timestamp, Object metadata);

}
