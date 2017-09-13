package org.apache.hadoop.hbase.consumer.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;

import com.alibaba.fastjson.JSON;
import com.lppz.configuration.es.BaseEsParamTypeEvent;
import com.lppz.elasticsearch.disruptor.BaseEsLogEventCRUD2Sender;
import com.lppz.util.kafka.consumer.listener.BaseByteKafkaConsumerListener;
//@Composnent("idxHBaseKafkaEsRegionServerConsumerListener")
public class IdxHBaseKafkaEsRegionServerFastConsumerListener extends BaseByteKafkaConsumerListener<EsIdxHbaseType> {
	
	protected static final Log logger = LogFactory
			.getLog(IdxHBaseKafkaEsRegionServerFastConsumerListener.class);
	
	private BaseEsLogEventCRUD2Sender logSender=BaseEsLogEventCRUD2Sender.create(5000, 1000);
	
	@Override
	protected void doMsg(EsIdxHbaseType t) {
		String esId=t.buildEsId();
		String idxName=HbaseUtil.buildEsIdx(t.getTbName(), t.getFamilyName(), t.getColumnName(), null);
		BaseEsParamTypeEvent type=new BaseEsParamTypeEvent();
		type.setEsOperType(t.getOp().name());
		type.setIdxName(idxName);
		type.setTypeName(EsIdxHbaseType.class.getName());
		type.setId(esId);
		String params=JSON.toJSONString(type);
		t.setOp(null);
		t.setMainColumn(null);
		t.setRowKey(null);
		t.setTbName(null);
		t.setColumnName(null);
		t.setFamilyName(null);
		String dto=JSON.toJSONString(t);
		logSender.sendMsg(dto, params);
	}
}