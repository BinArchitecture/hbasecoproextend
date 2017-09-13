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
package org.apache.hadoop.hbase.coprocessor.endpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.MultiThreadScan;
import org.apache.hadoop.hbase.client.coprocessor.exception.IdxHbaseCheckException;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType.Operation;
import org.apache.hadoop.hbase.client.coprocessor.model.ScanCond;
import org.apache.hadoop.hbase.client.coprocessor.model.ScanResult;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.HbaseDataType;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CascadeCell;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.StringList;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.kafka.producer.IdxHBaseKafkaEsRegionServerProducer;
import org.apache.hadoop.hbase.coprocessor.observer.IndexRegionObserver;
import org.apache.hadoop.hbase.coprocessor.util.BaseHbaseHttpClient;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.IdxHbaseProtos.IdxHbaseRequest;
import org.apache.hadoop.hbase.protobuf.generated.IdxHbaseProtos.IdxHbaseService;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;

import com.alibaba.fastjson.JSON;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * A concrete AggregateProtocol implementation. Its system level coprocessor
 * that computes the aggregate function at a region level.
 * {@link ColumnInterpreter} is used to interpret column value. This class is
 * parameterized with the following (these are the types with which the
 * {@link ColumnInterpreter} is parameterized, and for more description on
 * these, refer to {@link ColumnInterpreter}):
 */
@SuppressWarnings("deprecation")
@InterfaceAudience.Private
public class IdxHbaseImplementation extends IdxHbaseService
		implements CoprocessorService, Coprocessor {
	protected static final Log log = LogFactory
			.getLog(IdxHbaseImplementation.class);
	private RegionCoprocessorEnvironment env;
	private BaseHbaseHttpClient client;
	@Override
	public Service getService() {
		return this;
	}

	/**
	 * Stores a reference to the coprocessor environment provided by the
	 * {@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost} from
	 * the region where this coprocessor is loaded. Since this is a coprocessor
	 * endpoint, it always expects to be loaded on a table region, so always
	 * expects this to be an instance of {@link RegionCoprocessorEnvironment}.
	 * 
	 * @param env
	 *            the environment provided by the coprocessor host
	 * @throws IOException
	 *             if the provided environment is not an instance of
	 *             {@code RegionCoprocessorEnvironment}
	 */
	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		if (env instanceof RegionCoprocessorEnvironment) {
			this.env = (RegionCoprocessorEnvironment) env;
			RecoverableZooKeeper rz = ((RegionCoprocessorEnvironment) env).getRegionServerServices().getZooKeeper()
					.getRecoverableZooKeeper();
			client=new BaseHbaseHttpClient();
			client.init(rz);
		} else {
			throw new CoprocessorException("Must be loaded on a table region!");
		}
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
		// nothing to do
	}

	@Override
	public void addIdx(RpcController controller, IdxHbaseRequest request,
			RpcCallback<AggregateResponse> done) {
			AggregateResponse response = null;
			InternalScanner scanner = null;
			Integer idxPos=null;
			try {
				Scan scan = ProtobufUtil.toScan(request.getScan());
				checkScanRequest(scan, request);
				scan.addFamily(Bytes.toBytes(request.getFamilyName()));
				StringList sl=JSON.parseObject(request.getColumns(), StringList.class);
				Set<String> colSet=new TreeSet<String>();
				colSet.addAll(sl.getQulifierList());
				if(sl.getHbasedataList()!=null){
					for(HbaseDataType q:sl.getHbasedataList())
						colSet.add(q.getQulifier());
				}
				for(String col:colSet)
				scan.addColumn(Bytes.toBytes(request.getFamilyName()), Bytes.toBytes(col));
				scanner = env.getRegion().getScanner(scan);
				List<Put> llPut=new ArrayList<Put>();
				List<Cell> results = new ArrayList<Cell>();
				boolean hasMoreRows = false;
				long k=0;
				HRegion h = (HRegion) env.getRegion();
				do {
					hasMoreRows = scanner.next(results);
					if(!results.isEmpty()){
						String[] rr=Bytes.toString(results.get(0).getRow()).split(Constants.SPLITTER);
						StringBuilder idxRow=new StringBuilder(rr[0]).append(Constants.SPLITTER);
						idxRow.append("i").append(Constants.SPLITTER).append(request.getIdxName()).append(Constants.SPLITTER);
						CascadeCell cc=new CascadeCell().build(results);
						if(sl.getHbasedataList()!=null){
							int i=0;
							for(HbaseDataType q:sl.getHbasedataList()){
								String value=cc.getQulifyerValueMap().get(q.getQulifier());
								String v=q.build(value);
								if(v!=null)
								idxRow.//append(q.getQulifier()).append(Constants.QSPLITTERORDERBY).
								append(v);
								if(i++==sl.getHbasedataList().size()-1)
									idxRow.append(Constants.QLIFIERSPLITTER);
								else
									idxRow.append(Constants.QSPLITTERORDERBY);
							}
						}
						for(Cell c:results){
							idxRow.append(Bytes.toString(c.getQualifier())).append(Constants.QSPLITTER).append(Bytes.toString(c.getValue())).append(Constants.SPLITTER);
						}
						idxRow.append(rr[request.getDestPos()]);
						if(!isMainTable(request.getFamilyName()))
						idxRow.append(Constants.SPLITTER).append(rr[rr.length-1]);
						if(idxPos==null){
							int i=0;
							for(String s:idxRow.toString().split(Constants.SPLITTER)){
								if(s.equals(rr[request.getDestPos()])){
									idxPos=i;
									break;
								}
								i++;
							}
						}
						Put put=new Put(Bytes.toBytes(idxRow.toString()));
						put.addColumn(Bytes.toBytes(request.getFamilyName()), Bytes.toBytes(Constants.IDXPREFIX),Bytes.toBytes(""));
						put.setAttribute(Constants.IDXHBASEPUT, "".getBytes());
						put.setDurability(Durability.ASYNC_WAL);
						llPut.add(put);
						if(k++%10000==0){
							h.batchMutate(llPut.toArray(new Put[]{}));
							log.info(request.getFamilyName()+"."+request.getIdxName()+"."+llPut.size()+" has been insertd!");
							llPut.clear();
						}
					}
					results.clear();
				} while (hasMoreRows);
				if (!llPut.isEmpty()) {
					h.batchMutate(llPut.toArray(new Put[]{}));
					llPut.clear();
					ByteString first = ByteString.copyFromUtf8(idxPos==null?"0":String.valueOf(idxPos));
					AggregateResponse.Builder pair = AggregateResponse.newBuilder();
					pair.addFirstPart(first);
					ByteString second = ByteString.copyFromUtf8(String.valueOf(k));
					pair.setSecondPart(second);
					response = pair.build();
				}
			} catch (IOException e) {
				ResponseConverter.setControllerException(controller, e);
			} finally {
				if (scanner != null) {
					try {
						scanner.close();
					} catch (IOException ignored) {
					}
				}
			}
			done.run(response);
	}

	private boolean isMainTable(String familyName) {
		return familyName.equals(env.getRegion().getTableDesc().getValue(Constants.HBASEMAINTABLE));
	}

	private void checkScanRequest(Scan scan, IdxHbaseRequest request) throws IdxHbaseCheckException {
		if(request.getColumns()==null||request.getDestPos()<=0
				||request.getFamilyName()==null||request.getIdxName()==null)
			throw new IdxHbaseCheckException(
					"idx req invalid");
	}

	@Override
	public void dropIdx(RpcController controller, IdxHbaseRequest request,
			RpcCallback<AggregateResponse> done) {
		AggregateResponse response = null;
		InternalScanner scanner = null;
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());
			checkDropScanRequest(request);
			scan.addColumn(request.getFamilyName().getBytes(),Bytes.toBytes(Constants.IDXPREFIX));
			scanner = env.getRegion().getScanner(scan);
			List<Delete> llDel=new ArrayList<Delete>();
			List<Cell> results = new ArrayList<Cell>();
			boolean hasMoreRows = false;
			HRegion h = (HRegion) env.getRegion();
			long k=0;
			do {
				hasMoreRows = scanner.next(results);
				if(!results.isEmpty()){
					Delete del=new Delete(results.get(0).getRow());
					del.setAttribute(Constants.IDXHBASEDEL, "".getBytes());
					del.setDurability(Durability.ASYNC_WAL);
					llDel.add(del);
					if(k++%10000==0){
						h.batchMutate(llDel.toArray(new Delete[]{}));
						log.info(request.getFamilyName()+"."+request.getIdxName()+"."+llDel.size()+" has been deleted!");
						llDel.clear();
					}
				}
				results.clear();
			} while (hasMoreRows);
			if (!llDel.isEmpty()) {
				h.batchMutate(llDel.toArray(new Delete[]{}));
				llDel.clear();
				ByteString first = ByteString.copyFromUtf8(String.valueOf(k));
				AggregateResponse.Builder pair = AggregateResponse.newBuilder();
				pair.addFirstPart(first);
				response = pair.build();
			}
		} catch (IOException e) {
			ResponseConverter.setControllerException(controller, e);
		} finally {
			if (scanner != null) {
				try {
					scanner.close();
				} catch (IOException ignored) {
				}
			}
		}
		done.run(response);
	}

	private void checkDropScanRequest(IdxHbaseRequest request) throws IdxHbaseCheckException {
		if(request.getIdxName()==null||request.getFamilyName()==null)
			throw new IdxHbaseCheckException(
					"idx req invalid");
	}

	@Override
	public void addIdxTbData(RpcController controller, IdxHbaseRequest request,
			RpcCallback<AggregateResponse> done) {
		AggregateResponse response = null;
		InternalScanner scanner = null;
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());
			scan.addFamily(Bytes.toBytes(request.getFamilyName()));
			for(String col:request.getIdxcolumnList())
			scan.addColumn(Bytes.toBytes(request.getFamilyName()), Bytes.toBytes(col));
			scanner = env.getRegion().getScanner(scan);
			List<Cell> results = new ArrayList<Cell>();
			boolean hasMoreRows = false;
			IdxHBaseKafkaEsRegionServerProducer producer=IndexRegionObserver.getProducer();
			if(producer==null){
				producer=new IdxHBaseKafkaEsRegionServerProducer(env.getConfiguration());
			}
			long k=0;
			do {
				hasMoreRows = scanner.next(results);
				if(!results.isEmpty()){
					String rowKey=Bytes.toString(results.get(0).getRow());
					String[] rr=rowKey.split(Constants.SPLITTER);
					for(Cell c:results){
						EsIdxHbaseType p=new EsIdxHbaseType();
						p.setColumnName(Bytes.toString(c.getQualifier()));
						p.setFamilyName(request.getFamilyName());
						p.setIdxValue(Bytes.toString(c.getValue()));
						p.setOp(Operation.Insert);
						p.setPrefix(rr[0]);
						p.setRowKey(rowKey);
						p.setTbName(env.getRegion().getTableDesc().getNameAsString());
						producer.sendMsg(p, p.getRowKey());
						k++;
					}
				}
				results.clear();
			} while (hasMoreRows);
				ByteString first = ByteString.copyFromUtf8(String.valueOf(k));
				AggregateResponse.Builder pair = AggregateResponse.newBuilder();
				pair.addFirstPart(first);
				response = pair.build();
		} catch (IOException e) {
			ResponseConverter.setControllerException(controller, e);
		} catch (Exception e) {
			log.error(e.getMessage(),e);
		} finally {
			if (scanner != null) {
				try {
					scanner.close();
				} catch (IOException ignored) {
				}
			}
		}
		done.run(response);
	}

	@Override
	public void dropIdxTbData(RpcController controller,
			IdxHbaseRequest request, RpcCallback<AggregateResponse> done) {
		AggregateResponse response = null;
		InternalScanner scanner = null;
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());
			scanner = env.getRegion().getScanner(scan);
			List<Delete> llDel=new ArrayList<Delete>();
			List<Cell> results = new ArrayList<Cell>();
			boolean hasMoreRows = false;
			do {
				hasMoreRows = scanner.next(results);
				if(!results.isEmpty()){
					Delete del=new Delete(results.get(0).getRow());
					del.setAttribute(Constants.IDXHBASEDEL, "".getBytes());
					llDel.add(del);
				}
				results.clear();
			} while (hasMoreRows);
			if (!llDel.isEmpty()) {
//				hregionDel(llDel);
				ByteString first = ByteString.copyFromUtf8("0");
				AggregateResponse.Builder pair = AggregateResponse.newBuilder();
				pair.addFirstPart(first);
				response = pair.build();
			}
		} catch (IOException e) {
			ResponseConverter.setControllerException(controller, e);
		} finally {
			if (scanner != null) {
				try {
					scanner.close();
				} catch (IOException ignored) {
				}
			}
		}
		done.run(response);
	}

	@Override
	public void scanIdxRangeTb(RpcController controller,
			IdxHbaseRequest request, RpcCallback<AggregateResponse> done) {
		AggregateResponse response = null;
		InternalScanner scanner = null;
		try {
			Scan scan = ProtobufUtil.toScan(request.getScan());
			scan.addFamily(Bytes.toBytes(request.getFamilyName()));
			String tbName=env.getRegion().getTableDesc().getTableName().getNameAsString();
			RowKeyComposition rkc=client.getRKCFromMapCache(tbName, request.getFamilyName());
			for(String col:rkc.getFamilyColsNeedIdx())
			scan.addColumn(Bytes.toBytes(request.getFamilyName()), Bytes.toBytes(col));
			scanner = env.getRegion().getScanner(scan);
			List<Cell> results = new ArrayList<Cell>();
			boolean hasMoreRows = false;
			IdxHBaseKafkaEsRegionServerProducer producer=IndexRegionObserver.getProducer();
			if(producer==null){
				producer=new IdxHBaseKafkaEsRegionServerProducer(env.getConfiguration());
			}
			long k=0;
			do {
				hasMoreRows = scanner.next(results);
				if(!results.isEmpty()){
					String rowKey=Bytes.toString(results.get(0).getRow());
					String[] rr=rowKey.split(Constants.SPLITTER);
					for(Cell c:results){
						EsIdxHbaseType p=new EsIdxHbaseType();
						p.setColumnName(Bytes.toString(c.getQualifier()));
						p.setFamilyName(request.getFamilyName());
						p.setIdxValue(Bytes.toString(c.getValue()));
						p.setOp(Operation.Insert);
						p.setPrefix(rr[0]);
						p.setRowKey(rowKey);
						p.setTbName(env.getRegion().getTableDesc().getNameAsString());
						try {
							producer.sendMsg(p, p.getRowKey());
							k++;
						} catch (Exception e) {
							log.error(e.getMessage(),e);
						}
					}
				}
				results.clear();
			} while (hasMoreRows);
			ByteString first = ByteString.copyFromUtf8(String.valueOf(k));
			AggregateResponse.Builder pair = AggregateResponse.newBuilder();
			pair.addFirstPart(first);
			response = pair.build();
		} catch (IOException e) {
			ResponseConverter.setControllerException(controller, e);
		} finally {
			if (scanner != null) {
				try {
					scanner.close();
				} catch (IOException ignored) {
				}
			}
		}
		done.run(response);
	}
	
	public static void main(String[] args) {
//		String tableName="idx_omsorder";
//		System.out.println("$".compareTo("##"));
//		System.out.println(tableName.substring(tableName.indexOf(Constants.IDXTABLENAMEPREFIX)+Constants.IDXTABLENAMEPREFIX.length()));
	}
}
