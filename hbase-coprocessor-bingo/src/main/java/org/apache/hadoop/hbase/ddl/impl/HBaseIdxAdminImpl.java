package org.apache.hadoop.hbase.ddl.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.IdxHbaseClient;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbaseDDLException;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType;
import org.apache.hadoop.hbase.client.coprocessor.model.ddl.HBaseDDLResult;
import org.apache.hadoop.hbase.client.coprocessor.model.ddl.HBaseIdxParam;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.IdxKafkaMsg;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.IdxKafkaMsg.Operate;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaTableIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.StringList;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseStringUtil;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.client.kafka.IdxHBaseKafkaMetaProducer;
import org.apache.hadoop.hbase.ddl.AbstractHbaseDDLClient;
import org.apache.hadoop.hbase.ddl.HBaseIdxAdminInterface;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;

import com.alibaba.dubbo.config.annotation.Service;
import com.alibaba.fastjson.JSON;
import com.lppz.elasticsearch.EsModel;
import com.lppz.elasticsearch.EsModel.EsDMlEnum;
import com.lppz.elasticsearch.LppzEsComponent;
import com.lppz.elasticsearch.PrepareBulk;
import com.lppz.elasticsearch.query.SearchQuery;
import com.lppz.elasticsearch.query.fielditem.FieldItem;
import com.lppz.elasticsearch.query.fielditem.TermKvItem;
import com.lppz.elasticsearch.result.SearchResult;
@Service(protocol={"rest"},timeout=20000)
public class HBaseIdxAdminImpl extends AbstractHbaseDDLClient implements HBaseIdxAdminInterface,InitializingBean{
	private IdxHBaseKafkaMetaProducer mapProducer;
	private IdxHBaseKafkaMetaProducer zkProducer;
	private IdxHBaseKafkaMetaProducer idxLockProducer;
	private static final Logger logger = LoggerFactory.getLogger(HBaseIdxAdminImpl.class);
	public HBaseDDLResult addIdx(HBaseIdxParam param)
			throws Exception {
		HBaseDDLResult result=new HBaseDDLResult();
		try {
			Scan s=new Scan();
			StringList sl=new StringList(param.getColnameList());
			sl.setHbasedataList(param.getOrderByList());
			String colAndOrderBy=JSON.toJSONString(sl);
			lockUnlockIdxFamily(param.getTableName(),param.getFamilyName(),Operate.ADDIDXLOCK);
			Long[] pos=idxHbaseClient.addIdx(TableName.valueOf(param.getTableName()), param.getIdxName(), param.getFamilyName(), colAndOrderBy,s);
			sendIdxAddMsg(param.getTableName(), param.getIdxName(), param.getFamilyName(), sl, Integer.parseInt(String.valueOf(pos[0])),
					Integer.parseInt(String.valueOf(pos[1])));
			Thread.sleep(3000);
			result.setResult("add idx "+param.getTableName()+"."+param.getIdxName()+":"+pos[2]+" success!");
			return result;
		} catch (Throwable e) {
			logger.error(e.getMessage(),e);
			result.setExcp(new HbaseDDLException(e.getMessage()));
			return result;
		}
		finally{
			lockUnlockIdxFamily(param.getTableName(),param.getFamilyName(),Operate.RELEASEIDXLOCK);
		}
	}
	
	private void lockUnlockIdxFamily(String tableName, String familyName, Operate op) {
		IdxKafkaMsg t=new IdxKafkaMsg();
		t.setOp(op);
		t.setFamilyName(familyName);
		t.setTbName(tableName);
		try {
			idxLockProducer.sendMsg(t);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
	}

	public HBaseDDLResult addIdxTable(HBaseIdxParam param) throws Exception {
		HBaseDDLResult result=new HBaseDDLResult();
		try {
			HTableDescriptor hdt = check(param.getTableName(), param.getFamilyName());
			RowKeyComposition rkc=JSON.parseObject(hdt.getValue(param.getFamilyName()),RowKeyComposition.class);
			if(StringUtils.isNotBlank(param.getColumn())){
				param.setColumn(HbaseStringUtil.formatStringOrginal(param.getColumn()));
			}
			LinkedHashSet<String> lhs=new LinkedHashSet<String>();
			lhs.add(param.getColumn());
			if(rkc.getFamilyColsNeedIdx()==null||rkc.getFamilyColsNeedIdx().isEmpty()){
				rkc.setFamilyColsNeedIdx(lhs);
			}
			else{
				if(rkc.getFamilyColsNeedIdx().contains(param.getColumn())){
					result.setResult(param.getTableName()+"."+param.getFamilyName()+" already contains "+param.getColumn());
					return result;
				}
				rkc.getFamilyColsNeedIdx().add(param.getColumn());
			}
			hdt.setValue(param.getFamilyName(), JSON.toJSONString(rkc));
			lockUnlockIdxFamily(param.getTableName(),param.getFamilyName(),Operate.ADDIDXLOCK);
//		    createIdxTable(hdt,familyName,rkc,isCompress);
			Long num=idxHbaseClient.addIdxTbData(TableName.valueOf(param.getTableName()), param.getFamilyName(), lhs, new Scan());
			admin.modifyTable(param.getTableName(), hdt);
			result.setResult(param.getTableName()+"."+param.getFamilyName()+" addIdx:"+param.getColumn()+":"+num+" success!");
			return result;
		} catch (Throwable e) {
			logger.error(e.getMessage(),e);
			result.setExcp(new HbaseDDLException(e.getMessage()));
			return result;
		}
		finally{
			lockUnlockIdxFamily(param.getTableName(),param.getFamilyName(),Operate.RELEASEIDXLOCK);
		}
	}

//	@SuppressWarnings("deprecation")
//	private void createIdxTable(HTableDescriptor desc, String familyName,RowKeyComposition rkc,boolean isCompress) throws IOException {
//		if(!admin.tableExists(Constants.IDXTABLENAMEESPREFIX+desc.getTableName().getNameAsString())){
//			HTableDescriptor idxDesc = new HTableDescriptor(Constants.IDXTABLENAMEESPREFIX+desc.getTableName());
//			HColumnDescriptor colDesc = new HColumnDescriptor(familyName);
//	         colDesc.setMaxVersions(1);
//	         colDesc.setInMemory(true);
//	         colDesc.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
//	         if(isCompress){
//	         colDesc.setCompressionType(Algorithm.SNAPPY);
//	         colDesc.setCompactionCompressionType(Algorithm.SNAPPY);
//	         }
//			 idxDesc.addFamily(colDesc);
//			 byte[][] idxSplitKey=buildKey(rkc.getFamilyColsNeedIdx());
//			 admin.createTable(idxDesc, idxSplitKey);
//		}
//		else{
//			HTableDescriptor idxDesc = admin.getTableDescriptor(TableName.valueOf(Constants.IDXTABLENAMEESPREFIX+desc.getTableName().getNameAsString()));
//			//hbasetable add cf add region
//			if(!idxDesc.hasFamily(Bytes.toBytes(familyName))){
//				HBaseDDLAdminImpl ddl=new HBaseDDLAdminImpl();
//				FamilyCond fc=new FamilyCond(familyName,rkc);
//				ddl.modifyTableFamily(Constants.IDXTABLENAMEESPREFIX+desc.getTableName().getNameAsString(), HbaseOp.ADD, fc);
//			}
//		}
//	}

	private HTableDescriptor check(String tableName, String familyName) {
			try {
				AbstractHbaseDDLClient.initMapCache();
			} catch (IOException e) {
				logger.error(e.getMessage(),e);
			}
		HTableDescriptor hdt=mapCache.get(tableName);
		if(hdt==null)
			throw new IllegalStateException("no such table:"+tableName);
		String rrs=hdt.getValue(familyName);
		if(StringUtils.isBlank(rrs))
			throw new IllegalStateException("no such table:"+tableName+",family:"+familyName);
		return hdt;
	}
	
	public HBaseDDLResult dropIdxTable(HBaseIdxParam param)
					throws Exception {
		HBaseDDLResult result=new HBaseDDLResult();
		try {
			HTableDescriptor hdt = check(param.getTableName(), param.getFamilyName());
			RowKeyComposition rkc=JSON.parseObject(hdt.getValue(param.getFamilyName()),RowKeyComposition.class);
			LinkedHashSet<String> lhs=new LinkedHashSet<String>();
			lhs.add(param.getColumn());
			if(rkc.getFamilyColsNeedIdx()==null||rkc.getFamilyColsNeedIdx().isEmpty())
			{
				result.setResult(param.getTableName()+"."+param.getFamilyName()+" has no idx to drop!");
				return result;
			}
			else{
			   if(!rkc.getFamilyColsNeedIdx().contains(param.getColumn()))
			   {
				   result.setResult(param.getTableName()+"."+param.getFamilyName()+" already contains "+param.getColumn());
				   return result;
			   }
				rkc.getFamilyColsNeedIdx().removeAll(lhs);
			}
			hdt.setValue(param.getFamilyName(), JSON.toJSONString(rkc));
			lockUnlockIdxFamily(param.getTableName(),param.getFamilyName(),Operate.ADDIDXLOCK);
//			idxHbaseClient.dropIdxTbData(TableName.valueOf(Constants.IDXTABLENAMEESPREFIX+tableName), lhs,familyName);
			doDelHbaseIdxDataInEs(param.getTableName(),param.getFamilyName(),param.getColumn());
			admin.modifyTable(param.getTableName(), hdt);
			result.setResult(param.getTableName()+"."+param.getFamilyName()+" dropIdx:"+param.getColumn()+" success!");
			return result;
		} catch (Throwable e) {
			logger.error(e.getMessage(),e);
			result.setExcp(new HbaseDDLException(e.getMessage()));
			return result;
		}
		finally{
			lockUnlockIdxFamily(param.getTableName(),param.getFamilyName(),Operate.RELEASEIDXLOCK);
		}
	}

	private void doDelHbaseIdxDataInEs(String tableName, String familyName,
			String colname) {
		String idxName=Constants.IDXTABLENAMEESPREFIX+tableName+"-*";
		PrepareBulk prepareBulk=buildBulk();
		SearchQuery searchQuery=buildQuery(familyName,colname);
		LppzEsComponent.getInstance().scrollSearch(new String[]{idxName}, new String[]{EsIdxHbaseType.class.getName()}, searchQuery, 50000, 60000, prepareBulk);
		logger.info("drop "+idxName+"."+familyName+"."+colname+" success!");
	}

	private SearchQuery buildQuery(String familyName,String colname) {
		SearchQuery srq=new SearchQuery();
		srq.setFieldItemList(new ArrayList<FieldItem>(2));
		srq.getFieldItemList().add(new TermKvItem("familyName", familyName));
		srq.getFieldItemList().add(new TermKvItem("columnName", colname));
		return srq;
	}
	
	private PrepareBulk buildBulk() {
		PrepareBulk pp=new PrepareBulk(){
			@Override
			public void bulk(List<SearchResult> listRes) {
				List<EsModel> esModelList=new ArrayList<EsModel>(listRes.size());
				for(SearchResult sr:listRes){
					EsModel es=new EsModel(sr.getIndex(),sr.getType(),sr.getId(),sr.getSource(),EsDMlEnum.Delete);
					esModelList.add(es);
				}
				LppzEsComponent.getInstance().batchUpdateDelete(esModelList);
			}
		};
		return pp;
	}
	
	private void sendIdxAddMsg(String tableName, String idxName,
			String familyName, StringList colnameList, int destPos, int idxPos) {
		IdxKafkaMsg t=new IdxKafkaMsg();
		t.setOp(Operate.ADD);
		t.setColList(colnameList);
		t.setFamilyName(familyName);
		MetaIndex metaIndex=new MetaIndex();
		metaIndex.setIdxName(idxName);
		metaIndex.setDestPos(destPos);
		metaIndex.setIdxPos(idxPos);
		t.setMetaIndex(metaIndex);
		t.setTbName(tableName);
		try {
			mapProducer.sendMsg(t);
			zkProducer.sendMsg(t);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
	}

	public HBaseDDLResult dropIdx(HBaseIdxParam param) throws IOException {
		Scan scan=new Scan();
		HBaseDDLResult result=new HBaseDDLResult();
		try {
			lockUnlockIdxFamily(param.getTableName(),param.getFamilyName(),Operate.ADDIDXLOCK);
			sendIdxRemoveMsg(param.getTableName(),param.getFamilyName(),param.getIdxName());
			Long num=idxHbaseClient.dropIdx(TableName.valueOf(param.getTableName()), param.getIdxName(),param.getFamilyName(), scan);
			result.setResult(param.getTableName()+"."+param.getFamilyName()+" dropIdx:"+ param.getIdxName()+":"+num+" success!");
			return result;
		} catch (Throwable e) {
			logger.error(e.getMessage(),e);
			result.setExcp(new HbaseDDLException(e.getMessage()));
			return result;
		}
		finally{
			lockUnlockIdxFamily(param.getTableName(),param.getFamilyName(),Operate.RELEASEIDXLOCK);
		}
	}
	
	private void sendIdxRemoveMsg(String tableName, String familyName,
			String idxName) {
		IdxKafkaMsg t=new IdxKafkaMsg();
		t.setOp(Operate.DROP);
		t.setFamilyName(familyName);
		MetaIndex metaIndex=new MetaIndex();
		metaIndex.setIdxName(idxName);
		t.setMetaIndex(metaIndex);
		t.setTbName(tableName);
		try {
			mapProducer.sendMsg(t);
			zkProducer.sendMsg(t);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
		
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		this.mapProducer=new IdxHBaseKafkaMetaProducer(Constants.KAFKASYNCMAP_TOPIC);
		this.zkProducer=new IdxHBaseKafkaMetaProducer(Constants.KAFKASYNCZK_TOPIC);
		this.idxLockProducer=new IdxHBaseKafkaMetaProducer(Constants.KAFKAHBASELOCKED_TOPIC);		
	}

	@Override
	public HBaseDDLResult getIdx(HBaseIdxParam param) throws Exception {
		RecoverableZooKeeper rz=IdxHbaseClient.getRz();
		MetaTableIndex mti=HbaseUtil.initMetaIndex(rz);
		HBaseDDLResult result=new HBaseDDLResult();
		if(!mti.getTbIndexNameMap().containsKey(param.getTableName()))
			result.setExcp(new HbaseDDLException("no such table:"+param.getTableName()));
		if(!mti.getTbIndexNameMap().get(param.getTableName()).containsKey(param.getFamilyName()))
			result.setExcp(new HbaseDDLException("no such table or family:"+param.getTableName()+"."+param.getFamilyName()));
		result.setIdxMap(mti.getTbIndexNameMap().get(param.getTableName()).get(param.getFamilyName()).getMapqulifier());
		return result;
	}

	@Override
	public HBaseDDLResult existIdxData(List<EsIdxHbaseType> listId) throws Exception {
		if(CollectionUtils.isEmpty(listId))
			return null;
		List<String> idArray=new ArrayList<String>(listId.size());
		for(EsIdxHbaseType type:listId){
			idArray.add(type.buildEsId());
		}
		String index=Constants.IDXTABLENAMEESPREFIX+listId.get(0).getTbName()+"-*";
		String type=EsIdxHbaseType.class.getName();
		List<SearchResult> list=	LppzEsComponent.getInstance().searchMultiById(index, type, idArray, false);
		List<String> resultList=new ArrayList<String>();
		for(SearchResult sr:list)
			resultList.add(sr.getId());
		idArray.removeAll(resultList);
		HBaseDDLResult result=new HBaseDDLResult();
		result.setResultIdArray(idArray);
		return result;
	}
}