/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.coprocessor.observer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbaseAddColumnException;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbaseDeleteColumnException;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbaseDeleteTableException;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbaseForbidTruncateException;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbaseIdxMasterinitException;
import org.apache.hadoop.hbase.client.coprocessor.exception.kafka.HbaseKafkaInitException;
import org.apache.hadoop.hbase.client.coprocessor.exception.kafka.IdxHBaseKafkaMetaProducerException;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType.Operation;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.IdxKafkaMsg;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.IdxKafkaMsg.Operate;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaFamilyIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaFamilyPosIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaTableIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.kafka.consumer.IdxHbaseSyncMetaZKConsumer;
import org.apache.hadoop.hbase.coprocessor.kafka.consumer.listener.IdxHbaseSyncMetaZKConsumerListener;
import org.apache.hadoop.hbase.coprocessor.kafka.producer.IdxHBaseKafkaEsMasterProducer;
import org.apache.hadoop.hbase.coprocessor.kafka.producer.IdxHBaseKafkaMetaProducer;
import org.apache.hadoop.hbase.coprocessor.monitor.AdminMonitor;
import org.apache.hadoop.hbase.coprocessor.monitor.RegionMonitor;
import org.apache.hadoop.hbase.coprocessor.util.ShellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import com.alibaba.fastjson.JSON;
import com.lppz.util.http.BaseHttpClientsComponent;
import com.lppz.util.http.enums.HttpMethodEnum;
import com.lppz.util.kafka.consumer.listener.KafkaConsumerListener;

/**
 * 
 * Defines of coprocessor hooks(to support secondary indexing) of operations on
 * {@link org.apache.hadoop.hbase.master.HMaster} process.
 * 
 */
public class IndexMasterObserver extends BaseMasterObserver {

	private static final Log LOG = LogFactory.getLog(IndexMasterObserver.class
			.getName());
	public static String HOSTKEY = "hbase.bingo.hosts";
	public static String BINGO_MONITOR_TIME = "hbase.bingo.monitor.time";
	public static String RETION_MONITOR_TIME = "hbase.region.monitor.time";
	private static RecoverableZooKeeper rz;
	private static MetaTableIndex metaIndex;
	private IdxHBaseKafkaMetaProducer kafkaProd;
	private IdxHBaseKafkaEsMasterProducer kafkaEsProd;
	private Map<String, HostAndPort> hostPorts;
	@Override
	public void start(CoprocessorEnvironment ctx) throws IOException {
		if (rz == null) {
			synchronized (ctx) {
				if (rz == null) {
					if (ctx instanceof MasterCoprocessorEnvironment) {
						MasterCoprocessorEnvironment m = (MasterCoprocessorEnvironment) ctx;
						rz = m.getMasterServices().getZooKeeper()
								.getRecoverableZooKeeper();
						try {
							metaIndex=HbaseUtil.initMetaIndex(rz);
							initKafkaConsumer(m.getConfiguration(),metaIndex);
							kafkaProd=new IdxHBaseKafkaMetaProducer(Constants.KAFKASYNCFLUSH_TOPIC,m.getConfiguration());
							kafkaEsProd=new IdxHBaseKafkaEsMasterProducer(Constants.KAFKASYNCESMASTERSYNC_TOPIC,m.getConfiguration());
							startOtherUnit(m);
						} catch (KeeperException | InterruptedException e) {
							LOG.error(e.getMessage(),e);
							throw new HbaseIdxMasterinitException(e.getMessage());
						}
					}
				}
			}
		}
	}

	private void startOtherUnit(MasterCoprocessorEnvironment m) {
//		String zk = m.getConfiguration().get("zookeeper.connect");
		startBingo(m);
		startRegionMonitor(m);
	}
	
	private void startBingo(MasterCoprocessorEnvironment m) {
		//读site配置文件获取bingo节点，使用脚本遍历节点启动bingo微服务
		startAllBingoNodes(m);
		//启动监控，校验节点在zk上是否存活，如果不存在，校验端口是否存在，如果存在，创建zk节点；否则重启服务
		startBingoMonitor(m);
	}

	private void startAllBingoNodes(MasterCoprocessorEnvironment m) {
		String hosts = m.getConfiguration().get(HOSTKEY);
		String[] hostAndPorts = hosts.split(",");
		hostPorts = new HashMap<>();
		for (String hostAndPort : hostAndPorts) {
			HostAndPort hap = new HostAndPort(hostAndPort);
			hostPorts.put(hap.getName(), hap);
		}
		
		try {
			ShellUtil.execute("startBingoServerCluster.sh");
		} catch (IOException | InterruptedException | TimeoutException e) {
			LOG.error("启动bingoserver异常",e);
		}
	}
	
	private void startBingoMonitor(MasterCoprocessorEnvironment m) {
		try {
			int period = m.getConfiguration().getInt(BINGO_MONITOR_TIME, 30);
			new AdminMonitor(rz, hostPorts, 20, period).startMonitor();
		} catch (IOException | InterruptedException e) {
			LOG.error("启动bingo监控异常",e);
		}
	}

	private void startRegionMonitor(MasterCoprocessorEnvironment m){
		try {
			int period = m.getConfiguration().getInt(RETION_MONITOR_TIME, 30);
			new RegionMonitor(rz, 10, period).startMonitor();
		} catch (IOException | InterruptedException e) {
			LOG.error("启动region监控异常",e);
		}
	}
	
	@Override
	public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> ctx)
		      throws IOException {
		super.preShutdown(ctx);
		stopBingo();
	}

	private void stopBingo() {
		LOG.info("开始关闭bingo");
		BaseHttpClientsComponent client = new BaseHttpClientsComponent();
		for (Entry<String, HostAndPort> entry : hostPorts.entrySet()) {
			HostAndPort hap = entry.getValue();
			String url = "http://"+hap.getHost() + ":" +hap.getPort()+"/services/microservice/close";
			HttpRequestBase httpReqBase = client.createReqBase(url, HttpMethodEnum.POST);
			try {
				LOG.info(String.format("开始关闭,url=%s",url));
				client.doHttpSyncExec(httpReqBase);
			} catch (IOException e) {
				LOG.error("调用关闭bingo异常",e);
			}
		}
		client.closeHttpClient();
		LOG.info("完成关闭bingo");
	}

	@Override
	public synchronized void postCreateTable(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
//		if(desc.getTableName().getNameAsString().startsWith(Constants.IDXTABLENAMEPREFIX))
//			return;
		if(desc.getValue(Constants.DELIMITER_KEY)==null) return;
		if(metaIndex.getTbIndexNameMap().containsKey(desc.getTableName().getNameAsString()))
			return;
		if(regions!=null&&regions.length>0){
			desc.setValue(Constants.HBASEREGIONNUM, String.valueOf(regions.length));
			Map<String,String> map=new HashMap<String,String>(regions.length);
			for(HRegionInfo hregion:regions){
				map.put(Bytes.toString(hregion.getStartKey()), hregion.getRegionNameAsString());
			}
			desc.setValue(Constants.HBASEJEDISTABLEMAP, JSON.toJSONString(map));
			ctx.getEnvironment().getMasterServices().disableTable(desc.getTableName(), HConstants.NO_NONCE, HConstants.NO_NONCE);
			ctx.getEnvironment().getMasterServices().modifyTable(desc.getTableName(), desc, HConstants.NO_NONCE, HConstants.NO_NONCE);
			ctx.getEnvironment().getMasterServices().enableTable(desc.getTableName(),HConstants.NO_NONCE, HConstants.NO_NONCE);
//			HTableDescriptor idxDesc = new HTableDescriptor(Constants.IDXTABLENAMEPREFIX+desc.getTableName());
//			LinkedHashSet<String> setKey=null;
//			for(byte[] family:desc.getFamiliesKeys()){
//				RowKeyComposition rkc=JSON.parseObject(desc.getValue(Bytes.toString(family)),RowKeyComposition.class);
//				if(rkc.getFamilyColsNeedIdx()!=null){
//					setKey=rkc.getFamilyColsNeedIdx();
//					HColumnDescriptor colDesc = new HColumnDescriptor(family);
////					colDesc.setDataBlockEncoding(DataBlockEncoding.PREFIX);
////			         colDesc.setInMemory(true);
////			         colDesc.setMaxVersions(1);
//			         colDesc.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
//			         colDesc.setCompressionType(Algorithm.SNAPPY);
//			         colDesc.setCompactionCompressionType(Algorithm.SNAPPY);
//					 idxDesc.addFamily(colDesc);
//				}
//			}
//			if(setKey!=null){
//				byte[][] idxSplitKey=buildKey(setKey);
//				ctx.getEnvironment().getMasterServices().createTable(idxDesc, idxSplitKey, HConstants.NO_NONCE, HConstants.NO_NONCE);
//			}
		}
		MetaTableIndex mi ;
		try {
			mi = addmeta(desc,regions.length);
		} catch (KeeperException | InterruptedException e) {
			LOG.error(e.getMessage(),e);
			throw new IdxHBaseKafkaMetaProducerException(e.getMessage());
		}
		kafkaProd.sendMsg(mi);
		EsIdxHbaseType t=new EsIdxHbaseType();
		t.setOp(Operation.ADDTB);
		t.setTbName(desc.getTableName().getNameAsString());
		kafkaEsProd.sendMsg(t);
	}

//	private byte[][] buildKey(LinkedHashSet<String> setKey) {
//		if(CollectionUtils.isEmpty(setKey))
//		return null;
//		byte[][] bb=new byte[setKey.size()][];
//		int i=0;
//		Set<String> set=new HashSet<String>();
//		for(String s:setKey){
//			fillSet(set, s, new AtomicInteger(0));
//		}
//		for(String k:set){
//			bb[i++]=Bytes.toBytes(k);
//		}
//		return bb;
//	}
	
//	private void fillSet(Set<String> set,String s,AtomicInteger k){
//		if(k.get()+1>s.length())
//			return;
//		String c=s.substring(0,k.get()+1);
//		if(!set.contains(c)){
//			set.add(c);
//			return;
//		}
//		else{
//			k.addAndGet(1);
//			fillSet(set,s,k);
//		}
//	}
	 
	@Override
	public synchronized void postDeleteTable(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
//		if(tableName.getNameAsString().startsWith(Constants.IDXTABLENAMEPREFIX))
//			return;
		if(!metaIndex.getTbIndexNameMap().containsKey(tableName.getNameAsString()))
			return;
		MetaTableIndex mi;
		try {
			mi = modifymeta(tableName.getNameAsString(),
					Operate.DROP, null);
//			TableName idxTBName=TableName.valueOf(Constants.IDXTABLENAMEPREFIX+tableName.getNameAsString());
//			ctx.getEnvironment().getMasterServices().disableTable(idxTBName, HConstants.NO_NONCE, HConstants.NO_NONCE);
//			ctx.getEnvironment().getMasterServices().deleteTable(idxTBName, HConstants.NO_NONCE, HConstants.NO_NONCE);
			kafkaProd.sendMsg(mi);
			EsIdxHbaseType t=new EsIdxHbaseType();
			t.setOp(Operation.DROPTB);
			t.setTbName(tableName.getNameAsString());
			kafkaEsProd.sendMsg(t);
		} catch (KeeperException | InterruptedException e) {
			LOG.error(e.getMessage(),e);
			throw new HbaseDeleteTableException(e.getMessage());
		}
	}

	@Override
	public synchronized void postDeleteColumn(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, byte[] c) throws IOException {
//		if(tableName.getNameAsString().startsWith(Constants.IDXTABLENAMEPREFIX))
//			return;
		if(!(metaIndex.getTbIndexNameMap().get(tableName.getNameAsString())!=null&&
				metaIndex.getTbIndexNameMap().get(tableName.getNameAsString()).containsKey(Bytes.toString(c))))
			return;
		TableDescriptors tbs=ctx.getEnvironment().getMasterServices().getTableDescriptors();
		HTableDescriptor desc=tbs.get(tableName);
		if(desc.getValue(Bytes.toString(c))!=null){
			desc.remove(Bytes.toString(c));
			ctx.getEnvironment().getMasterServices().disableTable(desc.getTableName(), HConstants.NO_NONCE, HConstants.NO_NONCE);
			ctx.getEnvironment().getMasterServices().modifyTable(desc.getTableName(), desc, HConstants.NO_NONCE, HConstants.NO_NONCE);
			ctx.getEnvironment().getMasterServices().enableTable(desc.getTableName(),HConstants.NO_NONCE, HConstants.NO_NONCE);
		}
		MetaTableIndex mi=null;
		try {
			mi = modifymeta(tableName.getNameAsString(),
					Operate.DROPFAMILY, Bytes.toString(c));
			kafkaProd.sendMsg(mi);
			EsIdxHbaseType t=new EsIdxHbaseType();
			t.setOp(Operation.DROPCF);
			t.setTbName(tableName.getNameAsString());
			t.setFamilyName(Bytes.toString(c));
			kafkaEsProd.sendMsg(t);
		} catch (KeeperException | InterruptedException e) {
			LOG.error(e.getMessage(),e);
			throw new HbaseDeleteColumnException(e.getMessage());
		}
//		TableName idxTBName=TableName.valueOf(Constants.IDXTABLENAMEPREFIX+tableName.getNameAsString());
//		HTableDescriptor idxHd=tbs.get(idxTBName);
//		String rr=idxHd.getValue(Bytes.toString(c));
//		if(StringUtils.isBlank(rr))
//			return;
//		RowKeyComposition rkc=JSON.parseObject(rr,RowKeyComposition.class);
//		if(rkc.getFamilyColsNeedIdx()!=null){
//		ctx.getEnvironment().getMasterServices().disableTable(idxTBName, HConstants.NO_NONCE, HConstants.NO_NONCE);
//		ctx.getEnvironment().getMasterServices().deleteColumn(idxTBName, c, HConstants.NO_NONCE, HConstants.NO_NONCE);
//		ctx.getEnvironment().getMasterServices().enableTable(idxTBName,HConstants.NO_NONCE, HConstants.NO_NONCE);
//		}
	}

	@Override
	public synchronized void postAddColumn(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName, HColumnDescriptor column) throws IOException {
//		if(tableName.getNameAsString().startsWith(Constants.IDXTABLENAMEPREFIX))
//			return;
		if(metaIndex.getTbIndexNameMap().get(tableName.getNameAsString())!=null&&
				metaIndex.getTbIndexNameMap().get(tableName.getNameAsString()).containsKey(Bytes.toString(column.getName())))
			return;
		TableDescriptors tbs=ctx.getEnvironment().getMasterServices().getTableDescriptors();
		HTableDescriptor desc=tbs.get(tableName);
		String rkcString=column.getValue(Bytes.toString(column.getName()));
		if(!StringUtils.isBlank(rkcString)){
			desc.setValue(Bytes.toString(column.getName()), rkcString);
			ctx.getEnvironment().getMasterServices().disableTable(desc.getTableName(), HConstants.NO_NONCE, HConstants.NO_NONCE);
			ctx.getEnvironment().getMasterServices().modifyTable(desc.getTableName(), desc, HConstants.NO_NONCE, HConstants.NO_NONCE);
			ctx.getEnvironment().getMasterServices().enableTable(desc.getTableName(),HConstants.NO_NONCE, HConstants.NO_NONCE);
		}
		MetaTableIndex mi=null;
		try {
			mi = modifymeta(tableName.getNameAsString(),
					Operate.ADDFAMILY, Bytes.toString(column.getName()),rkcString);
			kafkaProd.sendMsg(mi);
			EsIdxHbaseType t=new EsIdxHbaseType();
			t.setOp(Operation.ADDCF);
			t.setTbName(tableName.getNameAsString());
			t.setFamilyName(Bytes.toString(column.getName()));
			kafkaEsProd.sendMsg(t);
		} catch (KeeperException | InterruptedException e) {
			LOG.error(e.getMessage(),e);
			throw new HbaseAddColumnException(e.getMessage());
		}
//		TableName idxTBName=TableName.valueOf(Constants.IDXTABLENAMEPREFIX+tableName.getNameAsString());
//		HTableDescriptor idxHd=tbs.get(idxTBName);
//		String rr=idxHd.getValue(Bytes.toString(column.getName()));
//		if(StringUtils.isBlank(rr))
//			return;
//		RowKeyComposition rkc=JSON.parseObject(rr,RowKeyComposition.class);
//		if(rkc.getFamilyColsNeedIdx()!=null){
//			ctx.getEnvironment().getMasterServices().disableTable(idxTBName, HConstants.NO_NONCE, HConstants.NO_NONCE);
//			ctx.getEnvironment().getMasterServices().addColumn(idxTBName, column, HConstants.NO_NONCE, HConstants.NO_NONCE);
//			ctx.getEnvironment().getMasterServices().enableTable(idxTBName,HConstants.NO_NONCE, HConstants.NO_NONCE);
//		}
	}

	private void initKafkaConsumer(Configuration cf,MetaTableIndex metaIndex) {
		IdxHbaseSyncMetaZKConsumer ihzkc = new IdxHbaseSyncMetaZKConsumer();
		KafkaConsumerListener<IdxKafkaMsg> kafkacListener = new IdxHbaseSyncMetaZKConsumerListener(
				metaIndex, rz);
		ihzkc.setKafkaListener(kafkacListener);
		try {
			ihzkc.doInit(cf);
		} catch (Exception e) {
			LOG.error(e.getMessage(),e);
			try {
				throw new HbaseKafkaInitException(e.getMessage());
			} catch (HbaseKafkaInitException e1) {
			}
		}
	}
	
	//not allow truncate table to avoid changing back to single region even if created table with pre multi-regions... 
	@Override
	public void preTruncateTable(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		if(metaIndex.getTbIndexNameMap().containsKey(tableName.getNameAsString()))
		throw new HbaseForbidTruncateException("can not truncate table:"+tableName.getNameAsString());
	}

	private MetaTableIndex modifymeta(String tableName,Operate op,String cfName, String... rkcString)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException, KeeperException, InterruptedException {
		if (Operate.DROP.equals(op)) {
			if (metaIndex.getTbIndexNameMap().containsKey(tableName)) {
				metaIndex.getTbIndexNameMap().remove(tableName);
				HbaseUtil.zkRmr(rz, Constants.ZOOINDEXPATH
						+ Constants.ZOOINDEXTBINDEXNAMEMAPPATH + "/"
						+ tableName);
			}
			if (metaIndex.getTbIndexMap().containsKey(tableName)) {
				metaIndex.getTbIndexMap().remove(tableName);
				HbaseUtil.zkRmr(rz, Constants.ZOOINDEXPATH
						+ Constants.ZOOINDEXTBINDEXMAPPATH + "/" + tableName);
			}
		}
		else if(Operate.DROPFAMILY.equals(op)){
			if(metaIndex.getTbIndexNameMap().get(tableName)!=null&&
					metaIndex.getTbIndexNameMap().get(tableName).containsKey(cfName)){
				metaIndex.getTbIndexNameMap().get(tableName).remove(cfName);
				rz.delete(Constants.ZOOINDEXPATH+Constants.ZOOINDEXTBINDEXNAMEMAPPATH+"/"+tableName+"/"+cfName, -1);
				}
			if(metaIndex.getTbIndexMap().get(tableName)!=null&&
					metaIndex.getTbIndexMap().get(tableName).containsKey(cfName)){
				metaIndex.getTbIndexMap().get(tableName).remove(cfName);
				rz.delete(Constants.ZOOINDEXPATH+Constants.ZOOINDEXTBINDEXMAPPATH+"/"+tableName+"/"+cfName, -1);
				}
		}
		else{
			MetaFamilyPosIndex mfpi=new MetaFamilyPosIndex();
			RowKeyComposition rkc=JSON.parseObject(rkcString[0],RowKeyComposition.class);
			mfpi.setDescpos(rkc.buildOidPos()+2);
			metaIndex.getTbIndexNameMap().get(tableName).put(cfName,mfpi);
			rz.create(Constants.ZOOINDEXPATH+Constants.ZOOINDEXTBINDEXNAMEMAPPATH+"/"+tableName+"/"+cfName,
					JSON.toJSONString(mfpi).getBytes(),
					Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
			
			MetaFamilyIndex mfi=new MetaFamilyIndex();
			metaIndex.getTbIndexMap().get(tableName).put(cfName, mfi);
			rz.create(Constants.ZOOINDEXPATH+Constants.ZOOINDEXTBINDEXMAPPATH+"/"+tableName+"/"+cfName,
					JSON.toJSONString(mfi).getBytes(),
					Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}
		return metaIndex;
	}
	
	private MetaTableIndex addmeta(HTableDescriptor desc, int length)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException, KeeperException, InterruptedException {
		String tableName=desc.getTableName().getNameAsString();
		Set<byte[]> set=desc.getFamiliesKeys();
		if(!metaIndex.getTbIndexNameMap().containsKey(tableName)&&
				!metaIndex.getTbIndexMap().containsKey(tableName)){
			Map<String,MetaFamilyPosIndex> mm=new HashMap<String,MetaFamilyPosIndex>(set.size());
			Map<String,MetaFamilyIndex> mmi=new HashMap<String,MetaFamilyIndex>(set.size());
			for(byte[] family:set){
				MetaFamilyPosIndex mfpi=new MetaFamilyPosIndex();
				RowKeyComposition rkc=JSON.parseObject(desc.getValue(Bytes.toString(family)),RowKeyComposition.class);
				mfpi.setDescpos(rkc.buildOidPos()+2);
				mm.put(Bytes.toString(family), mfpi);
				mmi.put(Bytes.toString(family),new MetaFamilyIndex());
			}
			metaIndex.getTbIndexNameMap().put(tableName, mm);
			metaIndex.getTbIndexMap().put(tableName,mmi);
			addMetaZk(tableName, mm, mmi);
		}
		return metaIndex;
	}

	private void addMetaZk(String tableName, Map<String, MetaFamilyPosIndex> mm,
			Map<String, MetaFamilyIndex> mmi) throws KeeperException,
			InterruptedException {
		rz.create(Constants.ZOOINDEXPATH+Constants.ZOOINDEXTBINDEXMAPPATH+"/"+tableName,
				null,
				Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		for(String family:mmi.keySet()){
			rz.create(Constants.ZOOINDEXPATH+Constants.ZOOINDEXTBINDEXMAPPATH+"/"+tableName+"/"+family,
					JSON.toJSONString(mmi.get(family)).getBytes(),
					Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}
		rz.create(Constants.ZOOINDEXPATH+Constants.ZOOINDEXTBINDEXNAMEMAPPATH+"/"+tableName,
				null,
				Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		for(String family:mm.keySet()){
			rz.create(Constants.ZOOINDEXPATH+Constants.ZOOINDEXTBINDEXNAMEMAPPATH+"/"+tableName+"/"+family,
					JSON.toJSONString(mm.get(family)).getBytes(),
					Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}
	}

	@Override
	public void postTruncateTable(
			ObserverContext<MasterCoprocessorEnvironment> ctx,
			TableName tableName) throws IOException {
		super.postTruncateTable(ctx, tableName);
//		if(tableName.getNameAsString().startsWith(Constants.IDXTABLENAMEPREFIX))
//			return;
//		TableName idxTBName=TableName.valueOf(Constants.IDXTABLENAMEPREFIX+tableName.getNameAsString());
//		if(ctx.getEnvironment().getMasterServices().getTableDescriptors().get(idxTBName)!=null){
//			ctx.getEnvironment().getMasterServices().disableTable(idxTBName, HConstants.NO_NONCE, HConstants.NO_NONCE);
//			ctx.getEnvironment().getMasterServices().truncateTable(idxTBName,false, HConstants.NO_NONCE, HConstants.NO_NONCE);
//			ctx.getEnvironment().getMasterServices().enableTable(idxTBName,HConstants.NO_NONCE, HConstants.NO_NONCE);
//		}
	}
}