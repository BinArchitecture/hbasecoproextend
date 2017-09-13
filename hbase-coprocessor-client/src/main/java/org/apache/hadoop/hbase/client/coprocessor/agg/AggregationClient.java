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

package org.apache.hadoop.hbase.client.coprocessor.agg;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.hadoop.hbase.client.coprocessor.model.agg.Sagg;
import org.apache.hadoop.hbase.client.coprocessor.model.groupby.Std;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AggregateGroupByProtos.AggTypeQulalifier;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.AggregateStgProtos.AggregateStgRequest;
import org.apache.hadoop.hbase.protobuf.generated.AggregateStgProtos.AggregateStgService;
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
public class AggregationClient implements Closeable {
	private static final Log log = LogFactory
			.getLog(AggregationClient.class);
	private final Connection connection;

	/**
	 * Constructor with Conf object
	 * 
	 * @param cfg
	 */
	public AggregationClient(Configuration cfg) {
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

	public <T> T max(final TableName tableName, String qulifier,String classType,
			 final Scan scan,Set<String> scanRange) throws Throwable {
		try (Table table = connection.getTable(tableName)) {
			return max(table, qulifier, classType,scan, scanRange);
		}
	}

	public <T> T max(final Table table, String qulifier,final String classType,
			final Scan scan,Set<String> scanRange) throws Throwable {
		return stg(table, qulifier, classType, scan, "max", new StgCallBack<T>(){
			@Override
			public synchronized void update(byte[] region, byte[] row, Sagg<T> sd) {
				if(sd==null)
					return;
				T rt =sd.getT();
				if(super.t == null){
					super.t = rt;
					return;
				}
				if(classType.equals(Double.class.getSimpleName())){
					BigDecimal bb=(BigDecimal)rt;
					BigDecimal sq=(BigDecimal)super.t;
					super.t = bb.compareTo(sq)>=0?rt:super.t;
				}
				else if(classType.equals(String.class.getSimpleName())){
					String bb=(String) rt;
					String sq=(String)super.t;
					super.t = bb.compareTo(sq)>=0?rt:super.t;
				}
				else if(classType.equals(Date.class.getSimpleName())){
					Date bb=(Date)rt;
					Date sq=(Date)super.t;
					super.t = bb.compareTo(sq)>=0?rt:super.t;
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

	public <T> T min(final TableName tableName, String qulifier,String classType,
			 final Scan scan,Set<String> scanRange) throws Throwable {
		try (Table table = connection.getTable(tableName)) {
			return min(table, qulifier, classType, scan, scanRange);
		}
	}

	public <T> T min(final Table table, String qulifier,final String classType,
			 final Scan scan,Set<String> scanRange) throws Throwable {
		return stg(table, qulifier, classType, scan, "min", new StgCallBack<T>(){
			@Override
			public synchronized void update(byte[] region, byte[] row, Sagg<T> sd) {
				if(sd==null)
					return;
				T rt =sd.getT();
				if(super.t == null){
					super.t = rt;
					return;
				}
				if(classType.equals(Double.class.getSimpleName())){
					BigDecimal bb=(BigDecimal)rt;
					BigDecimal sq=(BigDecimal)super.t;
					super.t = bb.compareTo(sq)<=0?rt:super.t;
				}
				else if(classType.equals(String.class.getSimpleName())){
					String bb=(String) rt;
					String sq=(String)super.t;
					super.t = bb.compareTo(sq)<=0?rt:super.t;
				}
				else if(classType.equals(Date.class.getSimpleName())){
					Date bb=(Date)rt;
					Date sq=(Date)super.t;
					super.t = bb.compareTo(sq)<=0?rt:super.t;
				}
				
			}
		}, scanRange);
	}

	public Integer rowCount(final TableName tableName,
			String qulifier,  final Scan scan,Set<String> scanRange) throws Throwable {
		try (Table table = connection.getTable(tableName)) {
			return rowCount(table, qulifier, scan, scanRange);
		}
	}

	public Integer rowCount(final Table table, String qulifier,
			 final Scan scan,Set<String> scanRange) throws Throwable {
		final AggregateStgRequest requestArg = validateArgAndGetPB(scan,
				qulifier,null, false,scanRange);
		
		
		StgCallBack<AtomicInteger> stgCallBack = new StgCallBack<AtomicInteger>(){
			@Override
			public void update(byte[] region, byte[] row, Sagg<AtomicInteger> sd) {
				if(t==null){
					synchronized (this) {
						if(t==null)
						t = new AtomicInteger(0);
					}
				}
				if(sd!=null)
				t.getAndAdd(sd.getT()==null?0:sd.getT().get());
			}
		};
		table.coprocessorService(AggregateStgService.class,
				scan.getStartRow(), scan.getStopRow(),
				new Batch.Call<AggregateStgService, Sagg<AtomicInteger>>() {
					@SuppressWarnings("unchecked")
					@Override
					public Sagg<AtomicInteger> call(AggregateStgService instance)
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
						Sagg<Integer> stg;
						try {
							stg = HbaseUtil.kyroDeSeriLize(b.toByteArray(), Sagg.class);
							return new Sagg<AtomicInteger>(new AtomicInteger(stg.getT()));
						} catch (Exception e) {
							e.printStackTrace();
							log.error(e.getMessage(),e);
						}
						return null;
					}
				}, stgCallBack);
		 return stgCallBack.getStgResult().get();
		
	}

	public BigDecimal sum(final TableName tableName, String qulifier,
			 final Scan scan,Set<String> scanRange) throws Throwable {
		try (Table table = connection.getTable(tableName)) {
			return sum(table, qulifier, scan, scanRange);
		}
	}

	public BigDecimal sum(final Table table, String qulifier,
			 final Scan scan,Set<String> scanRange) throws Throwable {
		return stg(table, qulifier, null, scan, "sum", new StgCallBack<BigDecimal>(){
			@Override
			public synchronized void update(byte[] region, byte[] row, Sagg<BigDecimal> sd) {
				if(t==null)
					t=BigDecimal.ZERO;
				if(sd!=null)
				t=t.add(sd.getT());
			}
		}, scanRange);
	}


	public Std avg(final Table table, String qulifier,
			 Scan scan,Set<String> scanRange) throws Throwable {
		final AggregateStgRequest requestArg = validateArgAndGetPB(scan,
				qulifier,null, false,scanRange);
		class StdCallBack implements Batch.Callback<Std>{
			private Std std=new Std(BigDecimal.ZERO,0);
			public Std getStd() {
				return std;
			}
			@Override
			public synchronized void update(byte[] region, byte[] row,
					Std result) {
				if(result!=null&&(result.getSum().compareTo(BigDecimal.ZERO)!=0||result.getTotal()!=0)){
					std=std.add(result);
				}
			}
		}
		StdCallBack callBack=new StdCallBack();
		 table.coprocessorService(
				 AggregateStgService.class, scan.getStartRow(),
					scan.getStopRow(),
					new Batch.Call<AggregateStgService, Std>() {
						@Override
						public Std call(AggregateStgService instance)
								throws IOException {
							ServerRpcController controller = new ServerRpcController();
							BlockingRpcCallback<AggregateResponse> rpcCallback = new BlockingRpcCallback<AggregateResponse>();
							instance.getAvg(controller, requestArg, rpcCallback);
							AggregateResponse response = rpcCallback.get();
							if (controller.failedOnException()) {
								throw controller.getFailedOn();
							}
							Std std=new Std(BigDecimal.ZERO,0);
							if (response.getFirstPartCount() == 0) {
								return std;
							}
							ByteString b = response.getFirstPart(0);
							try {
								std = HbaseUtil.kyroDeSeriLize(b.toByteArray(), Std.class);
							} catch (Exception e) {
								e.printStackTrace();
								log.error(e.getMessage(),e);
							}
							return std;
						}
					},callBack);
		return callBack.getStd();
	}

	public Std avg(final TableName tableName, String qulifier,
			 final Scan scan,Set<String> scanRange) throws Throwable {
		try (Table table = connection.getTable(tableName)) {
			return avg(table, qulifier, scan, scanRange);
		}
	}

	AggregateStgRequest validateArgAndGetPB(Scan scan, String qulifier,String classType,
			 boolean canFamilyBeAbsent, Set<String> scanRange) throws IOException {
		validateParameters(scan, canFamilyBeAbsent);
		final AggregateStgRequest.Builder requestBuilder = AggregateStgRequest
				.newBuilder();
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
	
	
	private <T>  T stg(final Table table, String qulifier,String classType,
			 final Scan scan, final String type,StgCallBack<T> stgCallBack,
			 Set<String> scanRange) throws Throwable {
		final AggregateStgRequest requestArg = validateArgAndGetPB(scan,
				qulifier,classType, false,scanRange);
		table.coprocessorService(AggregateStgService.class,
				scan.getStartRow(), scan.getStopRow(),
				new Batch.Call<AggregateStgService, Sagg<T>>() {
					@SuppressWarnings("unchecked")
					@Override
					public Sagg<T> call(AggregateStgService instance)
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
						Sagg<T> stg;
						try {
							stg = HbaseUtil.kyroDeSeriLize(b.toByteArray(), Sagg.class);
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
	
	private class StgCallBack<T> implements Batch.Callback<Sagg<T>> {
		T t;

		public T getStgResult() {
			return t;
		}

		@Override
		public void update(byte[] region, byte[] row, Sagg<T> t) {
		}
	}
}