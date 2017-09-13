package org.apache.hadoop.hbase.client.coprocessor.exception.kafka;

import org.apache.hadoop.hbase.DoNotRetryIOException;

public class JedisHbaseFlushConsumerListenerException extends DoNotRetryIOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7162766546716639309L;

	public JedisHbaseFlushConsumerListenerException(String message) {
		super(message);
	}

}
