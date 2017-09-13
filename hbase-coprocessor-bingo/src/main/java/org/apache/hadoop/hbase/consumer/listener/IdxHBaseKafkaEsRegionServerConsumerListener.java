package org.apache.hadoop.hbase.consumer.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType;
import org.apache.hadoop.hbase.ddl.AbstractHbaseDDLClient;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.lppz.configuration.es.BaseEsParamTypeEvent;
import com.lppz.elasticsearch.EsModel;
import com.lppz.elasticsearch.disruptor.BaseEsLogEventCRUD2Sender;
import com.lppz.util.disruptor.BaseErrorHandler;
import com.lppz.util.disruptor.sender.BaseClusterdDisruptorSender;
import com.lppz.util.disruptor.sender.BaseEvent2Sender;
import com.lppz.util.disruptor.sender.BaseEvent2SenderFactory;
import com.lppz.util.kafka.consumer.listener.BaseByteKafkaConsumerListener;
@Component("idxHBaseKafkaEsRegionServerConsumerListener")
public class IdxHBaseKafkaEsRegionServerConsumerListener extends BaseByteKafkaConsumerListener<EsIdxHbaseType> implements InitializingBean{
	
	protected static final Log logger = LogFactory
			.getLog(IdxHBaseKafkaEsRegionServerConsumerListener.class);
	
//	private BaseEsLogEventCRUD2Sender logSender=BaseEsLogEventCRUD2Sender.create(5000, 1000);
	
	@Value("${hbase.esClusterSize:1}")
	private int esClusterSize=1;
	
	private BaseClusterdDisruptorSender<String> logSender;
	
	@Override
	protected void doMsg(EsIdxHbaseType t) {
		String idxName=Constants.IDXTABLENAMEESPREFIX+t.getTbName()+"-";
		BaseEsParamTypeEvent type=new BaseEsParamTypeEvent();
		type.setEsOperType(t.getOp().name());
		type.setIdxName(idxName);
		type.setTypeName(EsIdxHbaseType.class.getName());
		String edId=t.buildEsId();
		type.setId(edId);
		String idxCurrday = buildCusufix(t, edId);
		type.setIdxCurrday(idxCurrday);
		String params=JSON.toJSONString(type);
		t.setOp(null);
		t.setMainColumn(null);
		t.setRowKey(null);
		t.setTbName(null);
		String dto=JSON.toJSONString(t);
		logSender.sendMsg(dto, params);
	}

	private String buildCusufix(EsIdxHbaseType t, String edId) {
		try {
			HTableDescriptor hdt=AbstractHbaseDDLClient.getMapCache().get(t.getTbName());	
			int estotal=Integer.parseInt(hdt.getValue(Constants.HBASEES2NDTOTAL));
			String idxCurrday=String.valueOf((Math.abs(edId.hashCode())%estotal));
			return idxCurrday;
		} catch (NumberFormatException e) {
			return "0";
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void afterPropertiesSet() throws Exception {
		if(AbstractHbaseDDLClient.getMapCache()==null||AbstractHbaseDDLClient.getMapCache().isEmpty())
		AbstractHbaseDDLClient.initMapCache();
		this.logSender=BaseClusterdDisruptorSender.build(new BaseEvent2SenderFactory<String>(){
			@Override
			public BaseEvent2Sender<String> build() {
				BaseErrorHandler<EsModel> errorHandler=new BaseErrorHandler<EsModel>(){
					@Override
					public void handler(EsModel u) {
					}

					@Override
					public int getRetryCount() {
						return Integer.MAX_VALUE;
					}

					@Override
					public boolean isLogInfo() {
						return true;
					}
				};
				return BaseEsLogEventCRUD2Sender.create(10000, 1000,errorHandler);
			}
		},esClusterSize);
	}

	public int getEsClusterSize() {
		return esClusterSize;
	}

	public void setEsClusterSize(int esClusterSize) {
		this.esClusterSize = esClusterSize;
	}
}