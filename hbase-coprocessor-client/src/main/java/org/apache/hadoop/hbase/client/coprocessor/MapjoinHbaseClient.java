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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.apache.hadoop.hbase.client.coprocessor.model.join.JoinString;
import org.apache.hadoop.hbase.client.coprocessor.model.join.ListCacadeCell;
import org.apache.hadoop.hbase.client.coprocessor.model.join.MapJoinRelationShip;
import org.apache.hadoop.hbase.client.coprocessor.model.join.TbJoinPoint;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.MapJoinHbaseProtos.MapJoinHbaseMultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.MapJoinHbaseProtos.MapJoinHbaseService;
import org.apache.hadoop.hbase.protobuf.generated.MapJoinHbaseProtos.MapJoinHbaseSingleRequest;
import org.apache.hadoop.hbase.protobuf.generated.MapJoinHbaseProtos.SecondeJoinReq;

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
public class MapjoinHbaseClient implements Closeable {
	private static final Log log = LogFactory.getLog(MapjoinHbaseClient.class);
	private Connection connection;
	public MapjoinHbaseClient() {}
	/**
	 * Constructor with Conf object
	 * 
	 * @param cfg
	 */
	public MapjoinHbaseClient(Configuration cfg) {
		try {
			// Create a connection on construction. Will use it making each of
			// the calls below.
			cfg.setLong("hbase.rpc.timeout", 6000000);
			cfg.setLong("hbase.client.scanner.caching", 1000);
			this.connection = ConnectionFactory.createConnection(cfg);
		} catch (Exception e) {
			log.error(e.getMessage(),e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		if (this.connection != null && !this.connection.isClosed()) {
			this.connection.close();
		}
	}

	static ExecutorService pool = Executors.newCachedThreadPool();
	public ListCacadeCell join(TbJoinPoint joinListCond) throws Throwable {
		final Map<String, List<JoinString>> mlp=joinListCond.buildMap(joinListCond.getTableName());
		final AtomicInteger ai=new AtomicInteger(0);
		Set<String> set=Collections.synchronizedSet(new HashSet<String>(1));
		for(final String tableName:mlp.keySet()){
			pool.execute(new JoinFirstPahse(tableName,mlp,ai,set));
		}
		while(true){
			Thread.sleep(1000);
			if(ai.get()>=mlp.size()){
				break;
			}
		}
		Table tb=connection.getTable(TableName.valueOf(joinListCond.getTableName()));
		final MapJoinRelationShip mj=joinListCond.buildRelationShip();
		String jedisPrefix=set.iterator().next();
		final SecondeJoinReq requestSencArg=validateArgAndGetPB(joinListCond.getScan(),mj,jedisPrefix);
		Map<byte[], ListCacadeCell> m = tb.coprocessorService(MapJoinHbaseService.class, null, null, new Batch.Call<MapJoinHbaseService, ListCacadeCell>() {
			@Override
			public ListCacadeCell call(MapJoinHbaseService instance)
					throws IOException {
				ServerRpcController controller = new ServerRpcController();
				BlockingRpcCallback<AggregateResponse> rpcCallback = new BlockingRpcCallback<AggregateResponse>();
				instance.joinSecondPhase(controller, requestSencArg,
						rpcCallback);
				AggregateResponse response = rpcCallback.get();
				if (controller.failedOnException()) {
					throw controller.getFailedOn();
				}
				if (response.getFirstPartCount() == 0) {
					return null;
				}
				ByteString b = response.getFirstPart(0);
				ListCacadeCell ll=null;
				try {
					ll = HbaseUtil.kyroDeSeriLize(b.toByteArray(), ListCacadeCell.class);
				} catch (Exception e) {
					e.printStackTrace();
					log.error(e.getMessage(),e);
				}
				return ll;
			}
		});
		ListCacadeCell lll=new ListCacadeCell();
		lll.setJedisPrefixNo(jedisPrefix);
		if(m!=null)
		for(ListCacadeCell lc:m.values()){
			if(lc!=null&&lc.getCascadeCellList()!=null)
			lll.getCascadeCellList().addAll(lc.getCascadeCellList());
		}
		try {
			tb.close();
		} catch (IOException e) {
		}
		return lll;
	}

	private class JoinFirstPahse implements Runnable{
		private String tableName;
		private Map<String, List<JoinString>> mlp;
		private AtomicInteger ai;
		private Set<String> set;
		
		public JoinFirstPahse(String tableName,Map<String, List<JoinString>> mlp,AtomicInteger ai,Set<String> set){
			this.tableName=tableName;
			this.mlp=mlp;
			this.ai=ai;
			this.set=set;
		}
		@Override
		public void run() {
			Table table=null;
			try {
				List<JoinString> lj=mlp.get(tableName);
				final MapJoinHbaseMultiRequest requestArg = validateArgAndGetPB(lj);
				table=connection.getTable(TableName.valueOf(tableName));
				set.add(table.coprocessorService(MapJoinHbaseService.class, null, null, new Batch.Call<MapJoinHbaseService, String>() {
					@Override
					public String call(MapJoinHbaseService instance)
							throws IOException {
						ServerRpcController controller = new ServerRpcController();
						BlockingRpcCallback<AggregateResponse> rpcCallback = new BlockingRpcCallback<AggregateResponse>();
						instance.joinFirstPhase(controller, requestArg,
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
				}).values().iterator().next());
			} catch (Throwable e) {
				log.error(e.getMessage(),e);
			} finally{
				ai.getAndAdd(1);
				try {
					table.close();
				} catch (IOException e) {
				}
			}
		}
	}
	
	MapJoinHbaseMultiRequest validateArgAndGetPB(List<JoinString> ll)
			throws IOException {
		MapJoinHbaseMultiRequest.Builder bb=MapJoinHbaseMultiRequest.newBuilder();
		for(JoinString js:ll){
			final MapJoinHbaseSingleRequest.Builder reqBuilder=MapJoinHbaseSingleRequest.newBuilder();
			if(js.getFamilyName()!=null)
			reqBuilder.setFamilyName(js.getFamilyName());
			if(js.getJoinPoint()!=null)
			reqBuilder.setJoinQulifier(js.getJoinPoint());
			reqBuilder.setScan(ProtobufUtil.toScan(js.getScan()));
			if(js.getQualColList()!=null)
			for(String q:js.getQualColList())
			reqBuilder.addQulifierCol(q);
			bb.addReq(reqBuilder.build());
		}
		bb.setHashRedisPrefix(UUID.randomUUID().toString());
		return bb.build();
	}
	
	SecondeJoinReq validateArgAndGetPB(Scan scan,MapJoinRelationShip mj, String set)
			throws IOException {
		SecondeJoinReq.Builder reqBuilder=SecondeJoinReq.newBuilder();
		if(mj!=null)
		reqBuilder.setJoinRelationShipMap(JSON.toJSONString(mj));
		if(set!=null)
		reqBuilder.setJoinRedisprefix(set);
		reqBuilder.setScan(ProtobufUtil.toScan(scan));
		return reqBuilder.build();
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
	
	public static void main(String[] args) {
		TbJoinPoint joinListCond=new TbJoinPoint();
		joinListCond.setTableName("B");
		joinListCond.setFamilyName("cfb");
		joinListCond.getJoinPointMap().put("a", new TbJoinPoint("A","cfa","a"));
		TbJoinPoint tc=new TbJoinPoint("C","cfc","c");
		tc.setScan(new Scan());
		tc.setQualColList(Arrays.asList(new String[]{"x1","x2","x3"}));
		TbJoinPoint tjpd=new TbJoinPoint("D","cfd","d1");
		tjpd.setScan(new Scan());
		tjpd.setQualColList(Arrays.asList(new String[]{"y1","y2","y3"}));
		tjpd.getJoinPointMap().put("d3", new TbJoinPoint("A","cfb","a1"));
		tjpd.getJoinPointMap().put("d4", new TbJoinPoint("F","cff","f1"));
		tc.getJoinPointMap().put("d1",tjpd);
		tc.getJoinPointMap().put("d2",new TbJoinPoint("E","cfg","ee"));
		joinListCond.getJoinPointMap().put("c", tc);
		TbJoinPoint td=new TbJoinPoint("D","cfd","d");
		td.getJoinPointMap().put("e", new TbJoinPoint("E","cfe","e"));
		joinListCond.getJoinPointMap().put("d",td);
		@SuppressWarnings("unused")
		Map<String, List<JoinString>> mlp=joinListCond.buildMap(joinListCond.getTableName());
		MapJoinRelationShip mj=joinListCond.buildRelationShip();
	}
}
