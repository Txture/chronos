package org.chronos.chronodb.api;

import org.chronos.common.exceptions.NotInstantiableException;

/**
 * A static collection of constants used throughout the {@link ChronoDB} API.
 *
 * <p>
 * This class is merely a container for the constants and must not be instantiated.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public final class ChronoDBConstants {

	/** The identifier for the master branch. The master branch is predefined and always exists. */
	public static final String MASTER_BRANCH_IDENTIFIER = "master";

	/** The name for the default keyspace. The default keyspace is predefined and always exists. */
	public static final String DEFAULT_KEYSPACE_NAME = "default";

	/** The file ending for incremental backup files (<b>C</b>hronos <b>I</b>ncremental <b>B</b>ackup).*/
	public static final String INCREMENTAL_BACKUP_FILE_ENDING = ".cib";

	private ChronoDBConstants() {
		throw new NotInstantiableException("This class must not be instantiated!");
	}

	public static final class IncrementalBackup {

		private IncrementalBackup(){
			throw new NotInstantiableException("This class must not be instantiated!");
		}

		public static final String METADATA_KEY_CHRONOS_VERSION = "org.chronos.chronodb.backup.incremental.chronos-version";
		public static final String METADATA_KEY_FORMAT_VERSION = "org.chronos.chronodb.backup.incremental.format-version";
		public static final String METADATA_KEY_REQUEST_START_TIMESTAMP = "org.chronos.chronodb.backup.incremental.request.start-timestamp";
		public static final String METADATA_KEY_REQUEST_PREVIOUS_WALL_CLOCK_TIME = "org.chronos.chronodb.backup.incremental.request.previous-wall-clock-time";
		public static final String METADATA_KEY_RESPONSE_WALL_CLOCK_TIME = "org.chronos.chronodb.backup.incremental.response.wall-clock-time";
		public static final String METADATA_KEY_RESPONSE_NOW = "org.chronos.chronodb.backup.incremental.response.now";
	}
}
