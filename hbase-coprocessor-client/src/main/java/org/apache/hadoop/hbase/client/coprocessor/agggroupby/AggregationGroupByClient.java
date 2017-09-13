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

package org.apache.hadoop.hbase.client.coprocessor.agggroupby;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.model.groupby.Avg;
import org.apache.hadoop.hbase.client.coprocessor.model.groupby.Count;
import org.apache.hadoop.hbase.client.coprocessor.model.groupby.Std;
import org.apache.hadoop.hbase.client.coprocessor.model.groupby.Stg;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AggregateGroupByProtos.AggTypeQulalifier;
import org.apache.hadoop.hbase.protobuf.generated.AggregateGroupByProtos.AggregateGroupByRequest;
import org.apache.hadoop.hbase.protobuf.generated.AggregateGroupByProtos.AggregateGroupByService;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateResponse;
import org.apache.hadoop.hbase.util.Bytes;

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
public class AggregationGroupByClient implements Closeable {
	private static final Log log = LogFactory
			.getLog(AggregationGroupByClient.class);
	private final Connection connection;

	/**
	 * Constructor with Conf object
	 * 
	 * @param cfg
	 */
	public AggregationGroupByClient(Configuration cfg) {
		try {
			// Create a connection on construction. Will use it making each of
			// the calls below.
			this.connection = ConnectionFactory.createConnection(cfg);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		if (this.connection != null && !this.connection.isClosed()) {
			this.connection.close();
		}
	}

	public <T> Map<String, T> max(final TableName tableName, String qulifier,String classType,
			List<String> groupby, final Scan scan,Set<String> scanRange) throws Throwable {
		try (Table table = connection.getTable(tableName)) {
			return max(table, qulifier, classType, groupby, scan, scanRange);
		}
	}

	public <T> Map<String, T> max(final Table table, String qulifier,final String classType,
			List<String> groupby, final Scan scan,Set<String> scanRange) throws Throwable {
		return stg(table, qulifier, classType, groupby, scan, "max", new StgCallBack<T>() {
			@Override
			public synchronized void update(byte[] region, byte[] row, Stg<T> stg) {
				if(stg==null)
					return;
				for (String s : stg.getMb().keySet()) {
					if(classType.equals(Double.class.getSimpleName())){
						T d = stgVal.get(s);
						Double bb=(Double)d;
						T st=stg.getMb().get(s);
						Double sq=(Double)st;
						if(bb==null){
							stgVal.put(s,st);
							continue;
						}
						stgVal.put(s,bb<=sq?st:d);
					}
					else if(classType.equals(String.class.getSimpleName())){
						T d = stgVal.get(s);
						String bb=(String)d;
						T st=stg.getMb().get(s);
						String sq=(String)st;
						if(bb==null){
							stgVal.put(s,st);
							continue;
						}
						stgVal.put(s,bb.compareTo(sq)<=0?st:d);
					}
					else if(classType.equals(Date.class.getSimpleName())){
						T d = stgVal.get(s);
						Date bb=(Date)d;
						T st=stg.getMb().get(s);
						Date sq=(Date)st;
						if(bb==null){
							stgVal.put(s,st);
							continue;
						}
						stgVal.put(s,bb.compareTo(sq)<=0?st:d);
					}
				}
			}
		}, scanRange);
	}

	/*
	 * @param scan
	 * 
	 * @param canFamilyBeAbsent whether column family can be absent in familyMap
	 * of scan
	 */
	private void validateParameters(Scan scan, boolean canFamilyBeAbsent)
			throws IOException {
		if (scan == null
				|| (Bytes.equals(scan.getStartRow(), scan.getStopRow()) && !Bytes
						.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW))
				|| ((Bytes.compareTo(scan.getStartRow(), scan.getStopRow()) > 0) && !Bytes
						.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW))) {
			throw new IOException(
					"Agg client Exception: Startrow should be smaller than Stoprow");
		} else if (!canFamilyBeAbsent) {
			if (scan.getFamilyMap().size() != 1) {
				throw new IOException("There must be only one family.");
			}
		}
	}

	public <T> Map<String, T> min(final TableName tableName, String qulifier,String classType,
			List<String> groupby, final Scan scan,Set<String> scanRange) throws Throwable {
		try (Table table = connection.getTable(tableName)) {
			return min(table, qulifier, classType, groupby, scan, scanRange);
		}
	}

	public <T> Map<String, T> min(final Table table, String qulifier,final String classType,
			List<String> groupby, final Scan scan,Set<String> scanRange) throws Throwable {
		return stg(table, qulifier, classType, groupby, scan, "min", new StgCallBack<T>() {
			@Override
			public synchronized void update(byte[] region, byte[] row, Stg<T> stg) {
				if(stg==null)
					return;
				for (String s : stg.getMb().keySet()) {
					if(classType.equals(Double.class.getSimpleName())){
						T d = stgVal.get(s);
						Double bb=(Double)d;
						T st=stg.getMb().get(s);
						Double sq=(Double)st;
						if(bb==null){
							stgVal.put(s,st);
							continue;
						}
						stgVal.put(s,bb>=sq?st:d);
					}
					else if(classType.equals(String.class.getSimpleName())){
						T d = stgVal.get(s);
						String bb=(String)d;
						T st=stg.getMb().get(s);
						String sq=(String)st;
						if(bb==null){
							stgVal.put(s,st);
							continue;
						}
						stgVal.put(s,bb.compareTo(sq)>=0?st:d);
					}
					else if(classType.equals(Date.class.getSimpleName())){
						T d = stgVal.get(s);
						Date bb=(Date)d;
						T st=stg.getMb().get(s);
						Date sq=(Date)st;
						if(bb==null){
							stgVal.put(s,st);
							continue;
						}
						stgVal.put(s,bb.compareTo(sq)>=0?st:d);
					}
				}
			}
		}, scanRange);
	}

	public Map<String, Integer> rowCount(final TableName tableName,
			String qulifier, List<String> groupby, final Scan scan,Set<String> scanRange) throws Throwable {
		try (Table table = connection.getTable(tableName)) {
			return rowCount(table, qulifier, groupby, scan, scanRange);
		}
	}

	public Map<String, Integer> rowCount(final Table table, String qulifier,
			List<String> groupby, final Scan scan,Set<String> scanRange) throws Throwable {
		final AggregateGroupByRequest requestArg = validateArgAndGetPB(scan,
				qulifier, null, groupby, false, scanRange);
		class RowNumCallback implements Batch.Callback<Count> {
			private final Map<String, Integer> rowCountMap = new ConcurrentHashMap<String, Integer>();

			public Map<String, Integer> getRowNumCount() {
				return rowCountMap;
			}

			@Override
			public synchronized void update(byte[] region, byte[] row, Count count) {
				if(count!=null)
				for (String s : count.getMi().keySet()) {
					Integer d = rowCountMap.get(s);
					rowCountMap.put(s,
							(d == null ? 0 : d) + (count.getMi().get(s)));
				}
			}
		}
		RowNumCallback rowNum = new RowNumCallback();
		table.coprocessorService(AggregateGroupByService.class,
				scan.getStartRow(), scan.getStopRow(),
				new Batch.Call<AggregateGroupByService, Count>() {
					@Override
					public Count call(AggregateGroupByService instance)
							throws IOException {
						ServerRpcController controller = new ServerRpcController();
						BlockingRpcCallback<AggregateResponse> rpcCallback = new BlockingRpcCallback<AggregateResponse>();
						instance.getRowNum(controller, requestArg, rpcCallback);
						AggregateResponse response = rpcCallback.get();
						if (controller.failedOnException()) {
							throw controller.getFailedOn();
						}
						if (response.getFirstPartCount() == 0) {
							return null;
						}
						ByteString b = response.getFirstPart(0);
						Count count=null;
						try {
							count = HbaseUtil.kyroDeSeriLize(b.toByteArray(), Count.class);
						} catch (Exception e) {
							e.printStackTrace();
							log.error(e.getMessage(),e);
						}
						return count;
					}
				}, rowNum);
		return rowNum.getRowNumCount();
	}

	public Map<String, Double> sum(final TableName tableName, String qulifier,
			List<String> groupby, final Scan scan,Set<String> scanRange) throws Throwable {
		try (Table table = connection.getTable(tableName)) {
			return sum(table, qulifier, groupby, scan, scanRange);
		}
	}

	public Map<String, Double> sum(final Table table, String qulifier,
			List<String> groupby, final Scan scan,Set<String> scanRange) throws Throwable {
		return stg(table, qulifier,null, groupby, scan, "sum", new StgCallBack<Double>() {
			@Override
			public synchronized void update(byte[] region, byte[] row, Stg<Double> stg) {
				if(stg==null)
					return;
				for (String s : stg.getMb().keySet()) {
					Double d = stgVal.get(s);
					stgVal.put(s, BigDecimal.valueOf(d == null ? 0d : d)
							.add(BigDecimal.valueOf(stg.getMb().get(s))).doubleValue());
				}
			}
		}, scanRange);
	}

	private <T> Map<String, T> stg(final Table table, String qulifier,String classType,
			List<String> groupby, final Scan scan, final String type,
			StgCallBack<T> stgCallBack,Set<String> scanRange) throws Throwable {
		final AggregateGroupByRequest requestArg = validateArgAndGetPB(scan,
				qulifier,classType,groupby, false,scanRange);
		table.coprocessorService(AggregateGroupByService.class,
				scan.getStartRow(), scan.getStopRow(),
				new Batch.Call<AggregateGroupByService, Stg<T>>() {
					@Override
					public Stg<T> call(AggregateGroupByService instance)
							throws IOException {
						ServerRpcController controller = new ServerRpcController();
						BlockingRpcCallback<AggregateResponse> rpcCallback = new BlockingRpcCallback<AggregateResponse>();
						if ("sum".equals(type))
							instance.getSum(controller, requestArg, rpcCallback);
						else if ("max".equals(type))
							instance.getMax(controller, requestArg, rpcCallback);
						else if ("min".equals(type))
							instance.getMin(controller, requestArg, rpcCallback);
						AggregateResponse response = rpcCallback.get();
						if (controller.failedOnException()) {
							throw controller.getFailedOn();
						}
						if (response.getFirstPartCount() == 0) {
							return null;
						}
						ByteString b = response.getFirstPart(0);
						Stg<T> stg;
						try {
							stg = HbaseUtil.kyroDeSeriLize(b.toByteArray(), Stg.class);
							return stg;
						} catch (Exception e) {
							e.printStackTrace();
							log.error(e.getMessage(),e);
						}
						return null;
					}
				}, stgCallBack);
		return stgCallBack.getStgResult();
	}

	public List<Avg> avg(final Table table, String qulifier,
			List<String> groupby, Scan scan,Set<String> scanRange) throws Throwable {
		List<Avg> listAvg = getAvgArgs(table, qulifier, groupby, scan, scanRange);
		return listAvg;
	}

	public List<Avg> avg(final TableName tableName, String qulifier,
			List<String> groupby, final Scan scan,Set<String> scanRange) throws Throwable {
		try (Table table = connection.getTable(tableName)) {
			return avg(table, qulifier, groupby, scan, scanRange);
		}
	}

	private List<Avg> getAvgArgs(final Table table,
			final String qulifier, List<String> groupby, final Scan scan,Set<String> scanRange)
			throws Throwable {
		final AggregateGroupByRequest requestArg = validateArgAndGetPB(scan,
				qulifier,null,groupby, false, scanRange);
		class AvgCallBack implements Batch.Callback<Map<String, Std>>{
			private List<Avg> list=Collections.synchronizedList(new ArrayList<Avg>());
			public List<Avg> getList() {
				return list;
			}
			@Override
			public void update(byte[] region, byte[] row,
					Map<String, Std> result) {
				if(result!=null){
					list.add(new Avg(result));
				}
			}
		}
		AvgCallBack callBack=new AvgCallBack();
		table.coprocessorService(
				AggregateGroupByService.class, scan.getStartRow(),
				scan.getStopRow(),
				new Batch.Call<AggregateGroupByService, Map<String, Std>>() {
					@Override
					public Map<String, Std> call(AggregateGroupByService instance)
							throws IOException {
						ServerRpcController controller = new ServerRpcController();
						BlockingRpcCallback<AggregateResponse> rpcCallback = new BlockingRpcCallback<AggregateResponse>();
						instance.getAvg(controller, requestArg, rpcCallback);
						AggregateResponse response = rpcCallback.get();
						if (controller.failedOnException()) {
							throw controller.getFailedOn();
						}
						Map<String, Std> mstd=null; 
						if (response.getFirstPartCount() == 0) {
							return mstd;
						}
						ByteString b = response.getFirstPart(0);
						try {
							Avg avg = HbaseUtil.kyroDeSeriLize(b.toByteArray(), Avg.class);
							return avg.getMm();
						} catch (Exception e) {
							e.printStackTrace();
							log.error(e.getMessage(),e);
						}
						return mstd;
					}
				},callBack);
		return callBack.getList();
	}

	AggregateGroupByRequest validateArgAndGetPB(Scan scan, String qulifier,String classType,
			List<String> groupby, boolean canFamilyBeAbsent, Set<String> scanRange) throws IOException {
		validateParameters(scan, canFamilyBeAbsent);
		final AggregateGroupByRequest.Builder requestBuilder = AggregateGroupByRequest
				.newBuilder();
		requestBuilder.addAllGroupby(groupby);
		if(qulifier!=null){
		AggTypeQulalifier.Builder builder=AggTypeQulalifier.newBuilder();
		builder.setQulifier(qulifier);
		if(classType!=null)
		builder.setClassType(classType);
		requestBuilder.setAggTypeQulalifier(builder.build());
		}
		requestBuilder.setScan(ProtobufUtil.toScan(scan));
		if(scanRange!=null)
			requestBuilder.addAllScanRange(scanRange);
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

	private class StgCallBack<T> implements Batch.Callback<Stg<T>> {
		Map<String, T> stgVal = new HashMap<String, T>();

		public Map<String, T> getStgResult() {
			return stgVal;
		}

		@Override
		public synchronized void update(byte[] region, byte[] row, Stg<T> stg) {
		}
	}
}
