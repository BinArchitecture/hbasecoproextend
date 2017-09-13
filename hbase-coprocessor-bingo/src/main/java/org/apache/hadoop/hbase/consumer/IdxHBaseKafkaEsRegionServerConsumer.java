package org.apache.hadoop.hbase.consumer;


import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import com.lppz.util.DubboPropertiesUtils;
import com.lppz.util.kafka.constant.KafkaConstant;
import com.lppz.util.kafka.consumer.BaseKafkaConsumer;
import com.lppz.util.kafka.consumer.listener.ByteKafkaConsumerListener;
@Component
public class IdxHBaseKafkaEsRegionServerConsumer extends BaseKafkaConsumer<EsIdxHbaseType> implements InitializingBean 
{
	@Override
	protected void initClazz() {
		super.setClazz(EsIdxHbaseType.class);
	}

	@Override
	protected void initTopic() {
		super.setTopic(Constants.KAFKASYNCESREGIONSERVERSYNC_TOPIC);
	}

	@Resource(name="idxHBaseKafkaEsRegionServerConsumerListener")
	public void setBytekafkaListener(ByteKafkaConsumerListener<EsIdxHbaseType> bytekafkaListener) {
		super.setBytekafkaListener(bytekafkaListener);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		super.setGroupId(Constants.KAFKAREGIONSERVERESSYNCZK_GROUP);
		super.setConsum_serilize_class(KafkaConstant.BYTEENCODER);
		String num=DubboPropertiesUtils.getKey("es.regionconsumer.threadnum");
		super.setTopic_patition_num(StringUtils.isEmpty(num)?"64":num);
		super.init();
	}   
}
