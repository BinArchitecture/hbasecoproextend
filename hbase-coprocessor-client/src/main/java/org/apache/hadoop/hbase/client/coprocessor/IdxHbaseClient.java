/*
 *
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

package org.apache.hadoop.hbase.client.coprocessor;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.BaseClientUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.model.ScanCond;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaFamilyPosIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaTableIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.ScanOrderStgKV;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.ComplexRowFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SimpleSubstringIdxComparator;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.IdxHbaseProtos.IdxHbaseRequest;
import org.apache.hadoop.hbase.protobuf.generated.IdxHbaseProtos.IdxHbaseService;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.KeeperException;

import com.alibaba.fastjson.JSON;
import com.google.protobuf.ByteString;

/**
 * This client class is for invoking the aggregate functions deployed on the
 * Region Server side via the AggregateGroupByService. This class will implement
 * the supporting functionality for summing/processing the individual results
 * obtained from the AggregateGroupByService for each region.
 * <p>
 * This will serve as the client side handler for invoking the aggregate
 * functions.
 * <ul>
 * For all aggregate functions,
 * <li>start row < end row is an essential condition (if they are not
 * {@link HConstants#EMPTY_BYTE_ARRAY})
 * <li>Column family can't be null. In case where multiple families are
 * provided, an IOException will be thrown. An optional column qualifier can
 * also be defined.
 * <li>For methods to find maximum, minimum, sum, rowcount, it returns the
 * parameter type. For average and std, it returns a double value. For row
 * count, it returns a long value.
 * <p>
 * Call {@link #close()} when done.
 */
@InterfaceAudience.Private
public class IdxHbaseClient implements Closeable {
	public static RecoverableZooKeeper getRz() {
		return rz;
	}

	private static final Log log = LogFactory.getLog(IdxHbaseClient.class);
	private final Connection connection;
	private static RecoverableZooKeeper rz;

	/**
	 * Constructor with Conf object
	 * 
	 * @param cfg
	 */
	public IdxHbaseClient(Configuration cfg, RecoverableZooKeeper rrz) {
		try {
			// Create a connection on construction. Will use it making each of
			// the calls below.
			cfg.setLong("hbase.rpc.timeout", 600000);
			cfg.setLong("hbase.client.scanner.caching", 1000);
			this.connection = ConnectionFactory.createConnection(cfg);
			if (rz == null) {
				synchronized (log) {
					if (rz == null) {
						if (rrz != null) {
							rz = rrz;
							return;
						}
						rz=BaseClientUtil.getZkFromHbaseConf(cfg,connection);
					}
				}
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		if (this.connection != null && !this.connection.isClosed()) {
			this.connection.close();
		}
	}

	public Long[] addIdx(final TableName tableName, String idxName,
			String familyName, String colnameList, final Scan scan)
			throws Throwable {
		try (Table table = connection.getTable(tableName)) {
			return addIdx(table, idxName, familyName, colnameList, scan);
		}
	}

	public Long[] addIdx(final Table table, String idxName, String familyName,
			String colnameList, final Scan scan) throws Throwable {
		int destPos = checkDuplicateIdxName(table.getName().getNameAsString(),
				familyName, idxName);
		Map<byte[], Long[]> m = null;
		try {
			final IdxHbaseRequest requestArg = validateArgAndGetPB(scan,
					idxName, familyName, colnameList, destPos, null);
			m = table.coprocessorService(IdxHbaseService.class,
					scan.getStartRow(), scan.getStopRow(),
					new Batch.Call<IdxHbaseService, Long[]>() {
						@Override
						public Long[] call(IdxHbaseService instance)
								throws IOException {
							ServerRpcController controller = new ServerRpcController();
							BlockingRpcCallback<AggregateResponse> rpcCallback = new BlockingRpcCallback<AggregateResponse>();
							instance.addIdx(controller, requestArg, rpcCallback);
							AggregateResponse response = rpcCallback.get();
							if (controller.failedOnException()) {
								throw controller.getFailedOn();
							}
							if (response.getFirstPartCount() == 0) {
								return null;
							}
							ByteString b = response.getFirstPart(0);
							Long idxPos = Long.parseLong(b.toStringUtf8());
							ByteString numString = response.getSecondPart();
							Long num = Long.parseLong(numString.toStringUtf8());
							return new Long[]{idxPos,num};
						}
					});
			Long[] tmpLong=filterAddIdxValue(colnameList, m);
			return new Long[] { (long)destPos,tmpLong[0],tmpLong[1]};
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			throw e;
		} finally {
			if (table != null)
				table.close();
		}
	}

	private Long[] filterAddIdxValue(String colnameList, Map<byte[], Long[]> m) {
		Long x = null;
		Long y = 0l;
		for (Iterator<Long[]> it = m.values().iterator(); it.hasNext();) {
			Long[] itt = it.next();
			Long i=itt[0];
			if (i != null) {
				x = i;
			}
			y+=itt[1];
		}
		if (x == null)
			throw new IllegalStateException("no data for " + colnameList
					+ ", can not add idx!");
		return new Long[]{x,y};
	}

	private int checkDuplicateIdxName(String tBName, String familyName,
			String idxName) throws Exception {
		MetaTableIndex metaIndex = null;
		try {
			metaIndex = HbaseUtil.initMetaIndex(rz);
		} catch (KeeperException | InterruptedException
				| UnsupportedEncodingException e) {
			log.error(e.getMessage(), e);
			throw e;
		}
		if (metaIndex.getTbIndexNameMap().get(tBName) == null)
			throw new IllegalStateException(tBName + " not exist!");
		if (metaIndex.getTbIndexNameMap().get(tBName).get(familyName) == null)
			throw new IllegalStateException(tBName + ":" + familyName
					+ " not exist!");
		for (String fa : metaIndex.getTbIndexNameMap().get(tBName).keySet()) {
			MetaFamilyPosIndex mfpi = metaIndex.getTbIndexNameMap().get(tBName)
					.get(fa);
			if (mfpi.getMapqulifier().containsKey(idxName))
				throw new IllegalStateException(tBName + ":" + fa + ":"
						+ idxName + " duplicated!");
		}
		int descPos=metaIndex.getTbIndexNameMap().get(tBName).get(familyName)
				.getDescpos();
		if(descPos<=0)
			throw new IllegalStateException(tBName + ":" + familyName + ":"
					+ idxName + " can not created bcz descPos le 0!");
		return descPos;
	}

	public Long dropIdx(final Table table, String idxName, String familyname,
			Scan scan) throws Throwable {
		try {
			FilterList fl = new FilterList();
			Filter filter = new ComplexRowFilter(CompareFilter.CompareOp.EQUAL,
					new SubstringComparator(idxName));
			fl.addFilter(filter);
			fl.addFilter(new FirstKeyOnlyFilter());
			scan.setFilter(fl);
			scan.setCaching(100);
			final IdxHbaseRequest requestArg = validateArgAndGetPB(scan,
					idxName, familyname, null, 0, null);
			Map<byte[], String> m = table.coprocessorService(
					IdxHbaseService.class, scan.getStartRow(),
					scan.getStopRow(),
					new Batch.Call<IdxHbaseService, String>() {
						@Override
						public String call(IdxHbaseService instance)
								throws IOException {
							ServerRpcController controller = new ServerRpcController();
							BlockingRpcCallback<AggregateResponse> rpcCallback = new BlockingRpcCallback<AggregateResponse>();
							instance.dropIdx(controller, requestArg,
									rpcCallback);
							AggregateResponse response = rpcCallback.get();
							if (controller.failedOnException()) {
								throw controller.getFailedOn();
							}
							if (response.getFirstPartCount() == 0) {
								return null;
							}
							ByteString b = response.getFirstPart(0);
							return b.toStringUtf8();
						}
					});
			if(m.values()!=null){
				long z=0l;
				for(String num:m.values()){
					z+=Long.parseLong(num);
				}
				return z;
			}
			return null;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			throw e;
		} finally {
			if (table != null)
				table.close();
		}
	}

	public Long dropIdx(final TableName tableName, String idxName,
			String familyname, Scan scan) throws Throwable {
		try (Table table = connection.getTable(tableName)) {
			return dropIdx(table, idxName, familyname, scan);
		}
	}

	public Long addIdxTbData(final TableName tableName, String familyName,
			LinkedHashSet<String> colnameList, final Scan scan)
			throws Throwable {
		try (Table table = connection.getTable(tableName)) {
			return addIdxTbData(table, familyName, colnameList, scan);
		}
	}

	public Long addIdxTbData(final Table table, String familyName,
			LinkedHashSet<String> colnameList, final Scan scan)
			throws Throwable {
		try {
			final IdxHbaseRequest requestArg = validateArgAndGetPB(scan, null,
					familyName, null, 0, colnameList);
			Map<byte[], String> m = table.coprocessorService(IdxHbaseService.class, scan.getStartRow(),
					scan.getStopRow(),
					new Batch.Call<IdxHbaseService, String>() {
						@Override
						public String call(IdxHbaseService instance)
								throws IOException {
							ServerRpcController controller = new ServerRpcController();
							BlockingRpcCallback<AggregateResponse> rpcCallback = new BlockingRpcCallback<AggregateResponse>();
							instance.addIdxTbData(controller, requestArg,
									rpcCallback);
							AggregateResponse response = rpcCallback.get();
							if (controller.failedOnException()) {
								throw controller.getFailedOn();
							}
							if (response.getFirstPartCount() == 0) {
								return null;
							}
							ByteString b = response.getFirstPart(0);
							return b.toStringUtf8();
						}
					});
			if(m.values()!=null){
				long z=0l;
				for(String num:m.values()){
					z+=Long.parseLong(num);
				}
				return z;
			}
			return null;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			throw e;
		} finally {
			if (table != null)
				table.close();
		}
	}

	public Long dropIdxTbData(final Table table,
			LinkedHashSet<String> colnameList, String familyname)
			throws Throwable {
		try {
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes(familyname));
			scan.addColumn(Bytes.toBytes(familyname),
					Bytes.toBytes(Constants.IDXPREFIX));
			FilterList fl = new FilterList();
			for (String col : colnameList) {
				fl.addFilter(new PrefixFilter((col+Constants.QLIFIERSPLITTER).getBytes()));
			}
			fl.addFilter(new FirstKeyOnlyFilter());
			scan.setFilter(fl);
			buildDelRange(scan, colnameList);
			final IdxHbaseRequest requestArg = validateArgAndGetPB(scan, null,
					familyname, null, 0, colnameList);
			Map<byte[], String> m = table.coprocessorService(
					IdxHbaseService.class, scan.getStartRow(),
					scan.getStopRow(),
					new Batch.Call<IdxHbaseService, String>() {
						@Override
						public String call(IdxHbaseService instance)
								throws IOException {
							ServerRpcController controller = new ServerRpcController();
							BlockingRpcCallback<AggregateResponse> rpcCallback = new BlockingRpcCallback<AggregateResponse>();
							instance.dropIdxTbData(controller, requestArg,
									rpcCallback);
							AggregateResponse response = rpcCallback.get();
							if (controller.failedOnException()) {
								throw controller.getFailedOn();
							}
							if (response.getFirstPartCount() == 0) {
								return null;
							}
							ByteString b = response.getFirstPart(0);
							return b.toStringUtf8();
						}
					});
			if(m.values()!=null){
				long z=0l;
				for(String num:m.values()){
					z+=Long.parseLong(num);
				}
				return z;
			}
			return null;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			throw e;
		} finally {
			if (table != null)
				table.close();
		}
	}

	public Long dropIdxTbData(final TableName tableName,
			LinkedHashSet<String> colnameList, String familyname)
			throws Throwable {
		try (Table table = connection.getTable(tableName)) {
			return dropIdxTbData(table, colnameList, familyname);
		}
	}

	public Long repairEsIdxData(final Table table, ScanOrderStgKV sosv,
			String familyname,ScanCond sc) throws Throwable {
		try {
			Scan scan = new Scan();
			FilterList fl=new FilterList();
			Filter rowFilter=new ComplexRowFilter(CompareOp.EQUAL, new SimpleSubstringIdxComparator(sosv));
			fl.addFilter(rowFilter);
			fl.addFilter(new FirstKeyOnlyFilter());
			scan.setFilter(fl);
			sosv.buildScanRange(scan, Constants.REGIDXHBASESTART,
					Constants.REGIDXHBASESTOP);
			final IdxHbaseRequest requestArg = validateArgAndGetPB(scan, null,
					familyname, sc==null?null:JSON.toJSONString(sc), 0, null);
			Map<byte[], String> m = table.coprocessorService(
					IdxHbaseService.class, scan.getStartRow(),
					scan.getStopRow(),
					new Batch.Call<IdxHbaseService, String>() {
						@Override
						public String call(IdxHbaseService instance)
								throws IOException {
							ServerRpcController controller = new ServerRpcController();
							BlockingRpcCallback<AggregateResponse> rpcCallback = new BlockingRpcCallback<AggregateResponse>();
							instance.scanIdxRangeTb(controller, requestArg,
									rpcCallback);
							AggregateResponse response = rpcCallback.get();
							if (controller.failedOnException()) {
								throw controller.getFailedOn();
							}
							if (response.getFirstPartCount() == 0) {
								return null;
							}
							ByteString b = response.getFirstPart(0);
							return b.toStringUtf8();
						}
					});
			if(m.values()!=null){
				long z=0l;
				for(String num:m.values()){
					z+=Long.parseLong(num);
				}
				return z;
			}
			return null;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			throw e;
		} finally {
			if (table != null)
				table.close();
		}
	}
	
	public List<String> scanIdxRangeTb(final Table table, ScanOrderStgKV sosv,
			String familyname,boolean isreverse) throws Throwable {
		try {
			Scan scan = new Scan();
			scan.setReversed(isreverse);
			FilterList fl=new FilterList();
			Filter rowFilter=new ComplexRowFilter(CompareOp.EQUAL, new SimpleSubstringIdxComparator(sosv));
			fl.addFilter(rowFilter);
			fl.addFilter(new FirstKeyOnlyFilter());
			scan.setFilter(fl);
			sosv.buildScanRange(scan, Constants.REGIDXHBASESTART,
					Constants.REGIDXHBASESTOP);
			final IdxHbaseRequest requestArg = validateArgAndGetPB(scan, null,
					familyname, null, 0, null);
			Map<byte[], List<String>> m = table.coprocessorService(
					IdxHbaseService.class, scan.getStartRow(),
					scan.getStopRow(),
					new Batch.Call<IdxHbaseService, List<String>>() {
						@SuppressWarnings("unchecked")
						@Override
						public List<String> call(IdxHbaseService instance)
								throws IOException {
							ServerRpcController controller = new ServerRpcController();
							BlockingRpcCallback<AggregateResponse> rpcCallback = new BlockingRpcCallback<AggregateResponse>();
							instance.scanIdxRangeTb(controller, requestArg,
									rpcCallback);
							AggregateResponse response = rpcCallback.get();
							if (controller.failedOnException()) {
								throw controller.getFailedOn();
							}
							if (response.getFirstPartCount() == 0) {
								return null;
							}
							ByteString listRange = response.getFirstPart(0);
							List<String> ll=null;
							try {
								ll = (List<String>) HbaseUtil.kyroDeSeriLize(listRange.toByteArray(),List.class);
							} catch (Exception e) {
								e.printStackTrace();
							}
							return ll;
						}
					});
			List<String> list=new ArrayList<String>();
			if(m!=null)
				for (List<String> ss : m.values()) {
					if(ss!=null){
						list.addAll(ss);
					}
				}
			return list;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			throw e;
		} finally {
			if (table != null)
				table.close();
		}
	}

	public List<String> scanIdxRangeTb(final TableName tableName,
			ScanOrderStgKV sosv, String familyname,boolean isreverse) throws Throwable {
		try (Table table = connection.getTable(tableName)) {
			return scanIdxRangeTb(table, sosv, familyname,isreverse);
		}
	}
	
	public Long repairEsIdxData(final TableName tableName,
			ScanOrderStgKV sosv, String familyname, ScanCond sc) throws Throwable {
		try (Table table = connection.getTable(tableName)) {
			return repairEsIdxData(table, sosv, familyname,sc);
		}
	}

	private void buildDelRange(Scan scan, LinkedHashSet<String> idxcolumnList) {
		String maxrow = Collections.max(idxcolumnList);
		String minrow = Collections.min(idxcolumnList);
		scan.setStartRow((minrow + "!").getBytes());
		scan.setStopRow((maxrow + "$").getBytes());
	}

	IdxHbaseRequest validateArgAndGetPB(Scan scan, String idxName,
			String familyName, String colnameList, int destPos,
			LinkedHashSet<String> idxTbCol) throws IOException {
		final IdxHbaseRequest.Builder requestBuilder = IdxHbaseRequest
				.newBuilder();
		if (colnameList != null)
			requestBuilder.setColumns(colnameList);
		if (destPos != 0)
			requestBuilder.setDestPos(destPos);
		if (familyName != null)
			requestBuilder.setFamilyName(familyName);
		if (idxName != null)
			requestBuilder.setIdxName(idxName);
		if (idxTbCol != null)
			requestBuilder.addAllIdxcolumn(idxTbCol);
		requestBuilder.setScan(ProtobufUtil.toScan(scan));
		return requestBuilder.build();
	}

	byte[] getBytesFromResponse(ByteString response) {
		ByteBuffer bb = response.asReadOnlyByteBuffer();
		bb.rewind();
		byte[] bytes;
		if (bb.hasArray()) {
			bytes = bb.array();
		} else {
			bytes = response.toByteArray();
		}
		return bytes;
	}
}