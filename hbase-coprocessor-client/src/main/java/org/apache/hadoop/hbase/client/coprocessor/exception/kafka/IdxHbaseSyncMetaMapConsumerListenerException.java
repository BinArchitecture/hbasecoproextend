package org.apache.hadoop.hbase.client.coprocessor.exception.kafka;

import org.apache.hadoop.hbase.DoNotRetryIOException;

public class IdxHbaseSyncMetaMapConsumerListenerException extends DoNotRetryIOException {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7943169494944372279L;

	public IdxHbaseSyncMetaMapConsumerListenerException(String message) {
		super(message);
	}
}
