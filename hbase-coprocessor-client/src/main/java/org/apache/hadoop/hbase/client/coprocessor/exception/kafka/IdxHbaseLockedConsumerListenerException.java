package org.apache.hadoop.hbase.client.coprocessor.exception.kafka;

import org.apache.hadoop.hbase.DoNotRetryIOException;

public class IdxHbaseLockedConsumerListenerException extends DoNotRetryIOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7162766546716639309L;

	public IdxHbaseLockedConsumerListenerException(String message) {
		super(message);
	}

}
