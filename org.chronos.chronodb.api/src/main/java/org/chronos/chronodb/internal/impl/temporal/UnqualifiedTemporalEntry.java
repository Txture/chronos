package org.chronos.chronodb.internal.impl.temporal;

import java.util.Arrays;

public class UnqualifiedTemporalEntry implements Comparable<UnqualifiedTemporalEntry> {

	private final UnqualifiedTemporalKey key;
	private final byte[] value;

	public UnqualifiedTemporalEntry(final UnqualifiedTemporalKey key, final byte[] value) {
		this.key = key;
		this.value = value;
	}

	public UnqualifiedTemporalKey getKey() {
		return this.key;
	}

	public byte[] getValue() {
		return this.value;
	}

	@Override
	public int compareTo(UnqualifiedTemporalEntry o) {
		if (o == null) {
			return 1;
		}
		return this.getKey().compareTo(o.getKey());
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("UnqualifiedTemporalEntry{");
		sb.append("key=").append(key);
		sb.append(", value=").append(Arrays.toString(value));
		sb.append('}');
		return sb.toString();
	}
}
