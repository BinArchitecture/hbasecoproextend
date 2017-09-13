package org.apache.hadoop.hbase.client.kafka;

import org.apache.hadoop.hbase.client.coprocessor.Constants;

import com.lppz.util.kafka.producer.BaseKafkaProducer;

public class JedisFlushHBaseKafkaProducer extends BaseKafkaProducer<String> {
	public JedisFlushHBaseKafkaProducer() throws Exception{
		try {
			super.init();
		} catch (Exception e) {
			throw e;
		}
	}

	@Override
	protected void resetTopic() {
		super.setTopic(Constants.HBASEJEDISTOPIC);
	}

	@Override
	protected Object generateMsg(String t) {
		return t;
	}
}
