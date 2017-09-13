package org.apache.hadoop.hbase.consumer;


import javax.annotation.Resource;

import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import com.lppz.util.kafka.consumer.BaseKafkaConsumer;
import com.lppz.util.kafka.consumer.listener.KafkaConsumerListener;
@Component
public class IdxHBaseKafkaEsMasterConsumer extends BaseKafkaConsumer<EsIdxHbaseType> implements InitializingBean
{
	@Resource(name="idxHBaseKafkaEsMasterConsumerListener")
	public void setKafkaListener(KafkaConsumerListener<EsIdxHbaseType> kafkaListener) {
		super.setKafkaListener(kafkaListener);
	}

	@Override
	protected void initClazz() {
		super.setClazz(EsIdxHbaseType.class);
	}

	@Override
	protected void initTopic() {
		super.setTopic(Constants.KAFKASYNCESMASTERSYNC_TOPIC);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		super.setGroupId(Constants.KAFKAMASTERESSYNCZK_GROUP);
		super.setTopic_patition_num("1");
		super.init();
	}   
}
