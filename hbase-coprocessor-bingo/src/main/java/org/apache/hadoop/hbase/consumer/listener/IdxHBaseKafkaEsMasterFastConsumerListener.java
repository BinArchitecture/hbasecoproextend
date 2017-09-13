package org.apache.hadoop.hbase.consumer.listener;

import java.io.IOException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType.Operation;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.ddl.AbstractHbaseDDLClient;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.lppz.elasticsearch.LppzEsComponent;
import com.lppz.util.kafka.consumer.listener.BaseKafkaConsumerListener;
//@Component("idxHBaseKafkaEsMasterConsumerListener")
public class IdxHBaseKafkaEsMasterFastConsumerListener extends BaseKafkaConsumerListener<EsIdxHbaseType> {
	
	private static final Logger logger = LoggerFactory.getLogger(IdxHBaseKafkaEsMasterFastConsumerListener.class);

	@Override
	protected void doMsg(EsIdxHbaseType t) {
		if(Operation.ADDTB.equals(t.getOp())){
//			boolean needAddIdxEs=false;
			try {
				AbstractHbaseDDLClient.initMapCache();
				HTableDescriptor hdt=AbstractHbaseDDLClient.getMapCache().get(t.getTbName());
				for(byte[] bf:hdt.getFamiliesKeys()){
					RowKeyComposition rkc=JSON.parseObject(hdt.getValue(Bytes.toString(bf)), RowKeyComposition.class);
					if(CollectionUtils.isNotEmpty(rkc.getFamilyColsNeedIdx())){
//						needAddIdxEs=true;
//						break;
						String crIdx=HbaseUtil.buildEsIdx(t.getTbName(), t.getFamilyName(), t.getColumnName(), null);
						LppzEsComponent.getInstance().createIndex(crIdx);
						logger.info("create idx "+crIdx+" success!");
					}
				}
			} catch (IOException e) {
				logger.error(e.getMessage(),e);
			}
//			if(needAddIdxEs){
//				String crIdx=Constants.IDXTABLENAMEESPREFIX+t.getTbName()+"-"+new SimpleDateFormat("yyyy").format(new Date());
//				LppzEsComponent.getInstance().createIndex(crIdx);
//				logger.info("create idx "+crIdx+" success!");
//			}
			logger.info("create tb "+t.getTbName()+" success!");
		}
		if(Operation.DROPTB.equals(t.getOp())){
			String idxName=Constants.IDXTABLENAMEESPREFIX+t.getTbName()+"-*";
			if(LppzEsComponent.getInstance().isIndexExists(idxName)){
				LppzEsComponent.getInstance().deleteIndex(idxName);
				logger.info("delete idx "+idxName+" success!");
			}
			logger.info("delete tb "+t.getTbName()+" success!");
		}
		else if(Operation.DROPCF.equals(t.getOp())){
//			PrepareBulk prepareBulk=buildBulk();
//			SearchQuery searchQuery=buildQuery(t);
//			LppzEsComponent.getInstance().scrollSearch(new String[]{idxName}, new String[]{EsIdxHbaseType.class.getName()}, searchQuery, 50000, 60000, prepareBulk);
//			logger.info("drop "+idxName+"."+t.getFamilyName()+" success!");
			String idxName=Constants.IDXTABLENAMEESPREFIX+t.getTbName()+"-"+t.getFamilyName()+"-*";
			if(LppzEsComponent.getInstance().isIndexExists(idxName)){
				LppzEsComponent.getInstance().deleteIndex(idxName);
				logger.info("delete idx "+idxName+" success!");
			}
		}
	}
	
//	private SearchQuery buildQuery(EsIdxHbaseType t) {
//		SearchQuery srq=new SearchQuery();
//		srq.setFieldItemList(new ArrayList<FieldItem>(1));
//		srq.getFieldItemList().add(new TermKvItem("familyName", t.getFamilyName()));
//		return srq;
//	}
//	
//	private PrepareBulk buildBulk() {
//		PrepareBulk pp=new PrepareBulk(){
//			@Override
//			public void bulk(List<SearchResult> listRes) {
//				List<EsModel> esModelList=new ArrayList<EsModel>(listRes.size());
//				for(SearchResult sr:listRes){
//					EsModel es=new EsModel(sr.getIndex(),sr.getType(),sr.getId(),sr.getSource(),EsDMlEnum.Delete);
//					esModelList.add(es);
//				}
//				LppzEsComponent.getInstance().batchUpdateDelete(esModelList);
//			}
//		};
//		return pp;
//	}
}