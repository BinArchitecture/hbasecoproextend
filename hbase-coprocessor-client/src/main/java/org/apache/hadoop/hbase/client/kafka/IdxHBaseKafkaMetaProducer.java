package org.apache.hadoop.hbase.client.kafka;

import org.apache.hadoop.hbase.client.coprocessor.model.idx.IdxKafkaMsg;

import com.lppz.util.kafka.producer.BaseKafkaProducer;



public class IdxHBaseKafkaMetaProducer extends BaseKafkaProducer<IdxKafkaMsg> {
	public IdxHBaseKafkaMetaProducer(String topic) throws Exception{
		super.setTopic(topic);
		try {
			super.init();
		} catch (Exception e) {
			throw e;
		}
	}

	@Override
	protected void resetTopic() {
		
	}

	@Override
	protected Object generateMsg(IdxKafkaMsg t) {
		return t;
	}
}
