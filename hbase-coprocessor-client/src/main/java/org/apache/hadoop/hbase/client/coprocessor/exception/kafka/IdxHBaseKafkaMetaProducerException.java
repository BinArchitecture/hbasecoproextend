package org.apache.hadoop.hbase.client.coprocessor.exception.kafka;

import org.apache.hadoop.hbase.DoNotRetryIOException;

public class IdxHBaseKafkaMetaProducerException extends DoNotRetryIOException {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7943169494944372279L;

	public IdxHBaseKafkaMetaProducerException(String message) {
		super(message);
	}
}
