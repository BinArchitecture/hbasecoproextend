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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.exception.kafka.HbaseKafkaInitException;
import org.apache.hadoop.hbase.client.coprocessor.model.join.JoinRelationShip;
import org.apache.hadoop.hbase.client.coprocessor.model.join.ListCacadeCell;
import org.apache.hadoop.hbase.client.coprocessor.model.join.MapJoinRelationList;
import org.apache.hadoop.hbase.client.coprocessor.model.join.MapJoinRelationShip;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CascadeCell;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.kafka.consumer.JedisHbaseFlushConsumer;
import org.apache.hadoop.hbase.coprocessor.kafka.consumer.listener.JedisHbaseFlushConsumerListener;
import org.apache.hadoop.hbase.coprocessor.util.RegionScanUtil;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.MapJoinHbaseProtos.MapJoinHbaseMultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.MapJoinHbaseProtos.MapJoinHbaseService;
import org.apache.hadoop.hbase.protobuf.generated.MapJoinHbaseProtos.MapJoinHbaseSingleRequest;
import org.apache.hadoop.hbase.protobuf.generated.MapJoinHbaseProtos.SecondeJoinReq;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;

import com.alibaba.fastjson.JSON;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.lppz.util.jedis.cluster.concurrent.OmsJedisCluster;
import com.lppz.util.kafka.consumer.listener.KafkaConsumerListener;

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
public class MapJoinHbaseImplementation extends MapJoinHbaseService
		implements CoprocessorService, Coprocessor {
	protected static final Log log = LogFactory
			.getLog(MapJoinHbaseImplementation.class);
	private RegionCoprocessorEnvironment env;
	private static OmsJedisCluster jedis;
	private static ExecutorService httpExecutor = Executors.newCachedThreadPool();
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
			if (jedis == null) {
				synchronized (httpExecutor) {
					if (jedis == null) {
						Configuration cf = env.getConfiguration();
						JedisPoolConfig jpoolConfig = getJedisPool(cf);
						Set<HostAndPort> nodes = getHostAndPortSet(cf);
						Integer timeout = cf.getInt("hbase.redis.timeout",
								50000);
						Integer maxRedirections = cf.getInt(
								"hbase.redis.maxRedirections", 5);
						jedis = new OmsJedisCluster(nodes, timeout,
								maxRedirections, jpoolConfig);
						initKafkaConsumer(cf);
					}
				}
			}
		} else {
			throw new CoprocessorException("Must be loaded on a table region!");
		}
	}

	private void initKafkaConsumer(Configuration cf) {
		JedisHbaseFlushConsumer ihsc = new JedisHbaseFlushConsumer();
		KafkaConsumerListener<String> kafkaListener = new JedisHbaseFlushConsumerListener(jedis);
		ihsc.setKafkaListener(kafkaListener);
		try {
			ihsc.doInit(cf);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			try {
				throw new HbaseKafkaInitException(e.getMessage());
			} catch (HbaseKafkaInitException e1) {
			}
		}
	}
	
	private Set<HostAndPort> getHostAndPortSet(Configuration cf) {
		Set<HostAndPort> nodes=new HashSet<HostAndPort>();
		String redisHostPort=cf.get("hbase.redis.hostport");
		if (StringUtils.isBlank(redisHostPort)) {
			throw new RuntimeException("jedis need a set of cluster nodes!!!");
		} else {
			for (String prop : redisHostPort.split(",")) {
				String[] hp=prop.split(":");
				nodes.add(new HostAndPort(hp[0],Integer.parseInt(hp[1])));
			}
		}
		return nodes;
	}

	private JedisPoolConfig getJedisPool(Configuration cf) {
		JedisPoolConfig jpc=new JedisPoolConfig();
		jpc.setBlockWhenExhausted(false);
		jpc.setNumTestsPerEvictionRun(100);
		jpc.setTestOnBorrow(false);
		jpc.setTestOnReturn(false);
		jpc.setMaxWaitMillis(cf.getInt("hbase.redis.maxWaitMillis", 10000));
		jpc.setMaxTotal(cf.getInt("hbase.redis.maxTotal", 30000));
		jpc.setMaxIdle(cf.getInt("hbase.redis.maxIdle", 5000));
		return jpc;
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
	}
	
	@Override
	public void joinFirstPhase(final RpcController controller,
			MapJoinHbaseMultiRequest request,
			RpcCallback<AggregateResponse> done) {
		String tmpprefix=null;
		if(jedis.hsetnx(Constants.HBASEHASHMAP,request.getHashRedisPrefix()+Constants.HBASEJEDISPREFIX,"1")==1){
			tmpprefix=Constants.HBASEJEDISPREFIX+jedis.incr(Constants.HBASECASCADEROW)+Constants.QSPLITTER;
			jedis.hset(Constants.HBASEHASHMAP,request.getHashRedisPrefix(), tmpprefix);
			//System.out.println("hit nx");
		}
		else{
			for(;;){
				tmpprefix=jedis.hget(Constants.HBASEHASHMAP,request.getHashRedisPrefix());
				if(tmpprefix!=null) break;
			}
			//System.out.println("hit nonx");
		}
		final String prefixJedis=tmpprefix; 
		AggregateResponse response = null;
		final AtomicInteger ai=new AtomicInteger(0);
		for(final MapJoinHbaseSingleRequest mhsr:request.getReqList()){
			httpExecutor.execute(new Runnable(){
				@Override
				public void run() {
					scanToRedis(mhsr,controller,prefixJedis);
					ai.getAndAdd(1);
				}
			});
		}
		while(true){
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
			if(ai.get()==request.getReqList().size()){
				break;
			}
		}
		AggregateResponse.Builder pair = AggregateResponse.newBuilder();
		pair.addFirstPart(ByteString.copyFromUtf8(request.getHashRedisPrefix()));
		response = pair.build();
		done.run(response);
	}

	private void scanToRedis(MapJoinHbaseSingleRequest request,RpcController controller, String prefixJedis) {
		Scan scan = null;
		RegionScanner rs=null;
		try {
			scan = ProtobufUtil.toScan(request.getScan());
			// checkScanRequest(scan, request);
			scan.addFamily(Bytes.toBytes(request.getFamilyName()));
			if (request.getQulifierColCount() > 0){
				for (String col : request.getQulifierColList())
					scan.addColumn(Bytes.toBytes(request.getFamilyName()),
							Bytes.toBytes(col));
					scan.addColumn(Bytes.toBytes(request.getFamilyName()),
						Bytes.toBytes(request.getJoinQulifier()));	
			}
			rs=RegionScanUtil.buildIdxScan(scan,env.getRegion());
			List<Cell> results = new ArrayList<Cell>();
			boolean hasMoreRows = false;
			do {
				hasMoreRows = rs.next(results);
				if (!results.isEmpty()) {
					byte[] key = generateKey(request, results, env.getRegion()
							.getTableDesc().getTableName().getNameAsString(),prefixJedis);
					if (key != null) {
						jedis.lpush(key,HbaseUtil.kyroSeriLize(new CascadeCell().build(results), -1));
					}
				}
				results.clear();
			} while (hasMoreRows);
		} catch (IOException e) {
			ResponseConverter.setControllerException(controller, e);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (IOException e) {
				}
		}
	}

	private byte[] generateKey(MapJoinHbaseSingleRequest request,
			List<Cell> results, String tbName, String prefixJedis) {
		StringBuilder sb=new StringBuilder(prefixJedis).append(tbName).append(Constants.QSPLITTER).append(request.getFamilyName()).
		append(Constants.QSPLITTER).append(request.getJoinQulifier()).append(Constants.QSPLITTER);
		for(Cell c:results){
			if(Bytes.toString(c.getQualifier()).equals(request.getJoinQulifier())){
				sb.append(Bytes.toString(c.getValue()));
				return sb.toString().getBytes();
			}
		}
		return null;
	}


	@Override
	public void joinSecondPhase(RpcController controller,
			SecondeJoinReq request, RpcCallback<AggregateResponse> done) {
		MapJoinRelationShip mrs=JSON.parseObject(request.getJoinRelationShipMap(), MapJoinRelationShip.class);
		Map<JoinRelationShip, JoinRelationShip> map=mrs.getMapJoin();
		AggregateResponse response = null;
		RegionScanner rs=null;
		Scan scan = null;
		ListCacadeCell ll=new ListCacadeCell();
		String joinPrefix=jedis.hget(Constants.HBASEHASHMAP,request.getJoinRedisprefix());
		try {
			scan = ProtobufUtil.toScan(request.getScan());
			// checkScanRequest(scan, request);
		    rs= RegionScanUtil.buildIdxScan(scan, env.getRegion());
			List<Cell> results = new ArrayList<Cell>();
			boolean hasMoreRows = false;
			do {
				hasMoreRows = rs.next(results);
				if (!results.isEmpty()) {
					ListCacadeCell l=new ListCacadeCell();
					mergeJoin(l,results,map,joinPrefix);
					ll.getCascadeCellList().addAll(l.getCascadeCellList());
				}
				results.clear();
			} while (hasMoreRows);
			if(ll.getCascadeCellList()!=null&&!ll.getCascadeCellList().isEmpty()){
				AggregateResponse.Builder pair = AggregateResponse.newBuilder();
				pair.addFirstPart(ByteString.copyFrom(HbaseUtil.kyroSeriLize(ll, -1)));
				response = pair.build();
				done.run(response);
			}
		} catch (IOException e) {
			ResponseConverter.setControllerException(controller, e);
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (IOException e) {
				}
		}
	}

	private void mergeJoin(ListCacadeCell ll, List<Cell> results,
			Map<JoinRelationShip, JoinRelationShip> map, String prefixKey) {
		CascadeCell c=new CascadeCell();
		String familyName=Bytes.toString(results.get(0).getFamily());
		String tbName=env.getRegion().getTableDesc().getTableName().getNameAsString();
		MapJoinRelationList mrlist=new MapJoinRelationList();
		Map<JoinRelationShip,List<CascadeCell>> mapJoin=new HashMap<JoinRelationShip,List<CascadeCell>>();
		for(String s:c.build(results).getQulifyerValueMap().keySet()){
			JoinRelationShip jr=map.get(new JoinRelationShip(tbName,familyName,s));
			if(jr==null)
				continue;
			String genKey=new StringBuilder(prefixKey).append(jr.getTableName()).append(Constants.QSPLITTER).
					append(jr.getFamilyName()).append(Constants.QSPLITTER)
					.append(jr.getJoinPoint()).append(Constants.QSPLITTER)
					.append(c.getQulifyerValueMap().get(s)).toString();
			if(jedis.exists(genKey.getBytes())){
				List<byte[]> lk=jedis.lrange(genKey.getBytes(),0,-1);
				List<CascadeCell> lcs=new ArrayList<CascadeCell>();
				for(byte[] b:lk){
					CascadeCell cc=null;
					try {
						cc = HbaseUtil.kyroDeSeriLize(b, CascadeCell.class);
						lcs.add(cc);
					} catch (Exception e) {
						log.error(e.getMessage(),e);
					}
				}
				if(!filter(jr,lcs,mapJoin,null)){
					return;
				}
			}
			else{
				return;
			}
		}
		genList(mrlist, mapJoin);
		genResult(mrlist,ll);
		for(Map<JoinRelationShip, String> mm:ll.getCascadeCellList()){
			for(String q:c.getQulifyerValueMap().keySet()){
				mm.put(new JoinRelationShip(tbName,familyName,q), c.getQulifyerValueMap().get(q));
			}
		}
	}

	private boolean filter(JoinRelationShip jr, List<CascadeCell> lcs,
			Map<JoinRelationShip, List<CascadeCell>> mapJoin,
			Map<String, List<CascadeCell>> redis) {
		if(jr.getRelationShipMap().isEmpty()){
			mapJoin.put(jr, lcs);
			return true;
		}
		List<Map<JoinRelationShip, String>> lm=genMap(jr,lcs);
		List<CascadeCell> lcc=new ArrayList<CascadeCell>();
	ak:	for(Map<JoinRelationShip, String> map:lm){
		Map<JoinRelationShip,List<CascadeCell>> mmap=new HashMap<JoinRelationShip,List<CascadeCell>>();
			for(JoinRelationShip jjr:map.keySet()){
//				if(new JoinRelationShip("C","cfc","c12").equals(jr)){
//					System.out.println("fuck");
//				}
				JoinRelationShip jrs=jr.getRelationShipMap().get(jjr);
				if(jrs==null)
					continue;
				String genKey=new StringBuilder(jrs.getTableName()).append(Constants.QSPLITTER).
						append(jrs.getFamilyName()).append(Constants.QSPLITTER)
						.append(jrs.getJoinPoint()).append(Constants.QSPLITTER)
						.append(map.get(jjr)).toString();
				if(redis!=null){
					List<CascadeCell> llcs=redis.get(genKey);
					if(llcs!=null){
						mmap.put(jrs, llcs);
					}
					else{
						continue ak;
					}
				}
				else{
					if(jedis.exists(genKey.getBytes())){
						List<byte[]> lk=jedis.lrange(genKey.getBytes(),0,-1);
						List<CascadeCell> llcs=new ArrayList<CascadeCell>();
						for(byte[] b:lk){
							CascadeCell cc=null;
							try {
								cc = HbaseUtil.kyroDeSeriLize(b, CascadeCell.class);
								llcs.add(cc);
							} catch (Exception e) {
								log.error(e.getMessage(),e);
							}
						}
						mmap.put(jrs, llcs);
					}
					else{
						continue ak;
					}
				}
			}
			for(JoinRelationShip jrs:mmap.keySet()){
				if(!filter(jrs,mmap.get(jrs),mapJoin,redis))
					return false;
			}
			lcc.add(convert(map));
		}
		if(lcc.isEmpty())
			return false;
		mapJoin.put(jr, lcc);
		return true;
	}

	private CascadeCell convert(Map<JoinRelationShip, String> map) {
		CascadeCell c=new CascadeCell();
		for(JoinRelationShip js:map.keySet())
			c.getQulifyerValueMap().put(js.getJoinPoint(), map.get(js));
		return c;
	}

	private void genResult(MapJoinRelationList mrlist, ListCacadeCell ll) {
		if(mrlist.getListJoin().isEmpty())
			return;
		if(ll.getCascadeCellList().isEmpty())
			for(Map<JoinRelationShip,String> map:mrlist.getListJoin()){
				ll.getCascadeCellList().add(map);
			}
		else{
			List<Map<JoinRelationShip, String>> l=new ArrayList<Map<JoinRelationShip, String>>();
			for(Map<JoinRelationShip,String> map:mrlist.getListJoin()){
				for(Map<JoinRelationShip,String> mapi:ll.getCascadeCellList()){
					Map<JoinRelationShip,String> m=new TreeMap<JoinRelationShip,String>();
					m.putAll(mapi);
					m.putAll(map);
					l.add(m);
				}
			}
			ll.setCascadeCellList(l);
		}
		genResult(mrlist.getMapJoinRelationList(),ll);
	}

	private void genList(MapJoinRelationList mrlist,
			Map<JoinRelationShip, List<CascadeCell>> mapJoin) {
		if(mapJoin.isEmpty())
			return;
		for(JoinRelationShip js:mapJoin.keySet()){
			List<CascadeCell> lcs=mapJoin.get(js);
			if(lcs==null)
				return;
			List<Map<JoinRelationShip, String>> lll=genMap(js,lcs);
			mrlist.setListJoin(lll);
			mapJoin.remove(js);
			mrlist.setMapJoinRelationList(new MapJoinRelationList());
			genList(mrlist.getMapJoinRelationList(),mapJoin);
		}
	}

	private List<Map<JoinRelationShip, String>> genMap(JoinRelationShip js,
			List<CascadeCell> lcs) {
		List<Map<JoinRelationShip, String>> lMap=new ArrayList<Map<JoinRelationShip, String>>();
		for(CascadeCell cc:lcs){
			Map<JoinRelationShip, String> map=new HashMap<JoinRelationShip, String>();
			for(String q:cc.getQulifyerValueMap().keySet()){
				map.put(new JoinRelationShip(js.getTableName(),js.getFamilyName(),q), cc.getQulifyerValueMap().get(q));
			}
			lMap.add(map);
		}
		return lMap;
	}
	
	public static void main(String[] args) {
		Map<JoinRelationShip,List<CascadeCell>> mapJoin=new ConcurrentHashMap<JoinRelationShip,List<CascadeCell>>();
		List<CascadeCell> lcs=new ArrayList<CascadeCell>();
		CascadeCell cc=new CascadeCell();
		cc.getQulifyerValueMap().put("a11", "a11");
		cc.getQulifyerValueMap().put("a12", "a12");
		CascadeCell cc1=new CascadeCell();
		cc1.getQulifyerValueMap().put("a11", "a21");
		cc1.getQulifyerValueMap().put("a12", "a22");
		lcs.add(cc);
		lcs.add(cc1);
		mapJoin.put(new JoinRelationShip("A","cfa",null),lcs);
		List<CascadeCell> lcs1=new ArrayList<CascadeCell>();
		CascadeCell cc2=new CascadeCell();
		cc2.getQulifyerValueMap().put("b11", "b11");
		cc2.getQulifyerValueMap().put("b12", "b12");
		CascadeCell cc3=new CascadeCell();
		cc3.getQulifyerValueMap().put("b11", "b21");
		cc3.getQulifyerValueMap().put("b12", "b22");
		CascadeCell cc4=new CascadeCell();
		cc4.getQulifyerValueMap().put("b11", "b31");
		cc4.getQulifyerValueMap().put("b12", "b32");
		lcs1.add(cc2);
		lcs1.add(cc3);
		lcs1.add(cc4);
		mapJoin.put(new JoinRelationShip("B","cfb",null),lcs1);
		List<CascadeCell> lcs2=new ArrayList<CascadeCell>();
		CascadeCell cc5=new CascadeCell();
		cc5.getQulifyerValueMap().put("c11", "c11");
		cc5.getQulifyerValueMap().put("c12", "cc");
		cc5.getQulifyerValueMap().put("c13", "cc3");
		CascadeCell cc6=new CascadeCell();
		cc6.getQulifyerValueMap().put("c11", "d21");
		cc6.getQulifyerValueMap().put("c12", "cc");
		cc6.getQulifyerValueMap().put("c13", "ccc5");
		CascadeCell cc7=new CascadeCell();
		cc7.getQulifyerValueMap().put("c11", "c31");
		cc7.getQulifyerValueMap().put("c12", "cc");
		cc7.getQulifyerValueMap().put("c13", "cc33");
		lcs2.add(cc5);
		lcs2.add(cc6);
		lcs2.add(cc7);
		JoinRelationShip jsc=new JoinRelationShip("C","cfc","c12");
		mapJoin.put(jsc,lcs2);
		JoinRelationShip jd=new JoinRelationShip("D","cfd","d11");
		jsc.getRelationShipMap().put(new JoinRelationShip("C","cfc","c11"),jd);
		jsc.getRelationShipMap().put(new JoinRelationShip("C","cfc","c13"),new JoinRelationShip("F","cff","f11"));
		jd.getRelationShipMap().put(new JoinRelationShip("D","cfd","d12"), new JoinRelationShip("E","cfe","e12"));
		Map<String,List<CascadeCell>> redis=new HashMap<String,List<CascadeCell>>();
		List<CascadeCell> llcs=new ArrayList<CascadeCell>();
		CascadeCell dd1=new CascadeCell();
		dd1.getQulifyerValueMap().put("d11", "d21");
		dd1.getQulifyerValueMap().put("d12", "d12");
		dd1.getQulifyerValueMap().put("d13", "d13");
		CascadeCell dd2=new CascadeCell();
		dd2.getQulifyerValueMap().put("d11", "d21");
		dd2.getQulifyerValueMap().put("d12", "d22");
		dd2.getQulifyerValueMap().put("d13", "d23");
		CascadeCell dd3=new CascadeCell();
		dd3.getQulifyerValueMap().put("d11", "d21");
		dd3.getQulifyerValueMap().put("d12", "d32");
		dd3.getQulifyerValueMap().put("d13", "d33");
		llcs.add(dd1);
		llcs.add(dd2);
		llcs.add(dd3);
		redis.put("D"+Constants.QSPLITTER+"cfd"+Constants.QSPLITTER+"d11"
		+Constants.QSPLITTER+"d21", llcs);
		List<CascadeCell> llcse=new ArrayList<CascadeCell>();
		List<CascadeCell> llcsf=new ArrayList<CascadeCell>();
		CascadeCell ee1=new CascadeCell();
		ee1.getQulifyerValueMap().put("e11", "e21");
		ee1.getQulifyerValueMap().put("e12", "d12");
		ee1.getQulifyerValueMap().put("e13", "d13");
		CascadeCell ee2=new CascadeCell();
		ee2.getQulifyerValueMap().put("e11", "e22");
		ee2.getQulifyerValueMap().put("e12", "d22");
		ee2.getQulifyerValueMap().put("e13", "e32");
		CascadeCell ff1=new CascadeCell();
		ff1.getQulifyerValueMap().put("f11", "cc5");
		ff1.getQulifyerValueMap().put("f12", "f12");
		ff1.getQulifyerValueMap().put("f13", "f23");
		CascadeCell ff2=new CascadeCell();
		ff2.getQulifyerValueMap().put("f11", "ccc5");
		ff2.getQulifyerValueMap().put("f12", "f22");
		ff2.getQulifyerValueMap().put("f13", "f33");
		llcse.add(ee1);
//		llcse.add(ee2);
		llcsf.add(ff1);
//		llcsf.add(ff2);
		redis.put("E"+Constants.QSPLITTER+"cfe"+Constants.QSPLITTER+"e12"
				+Constants.QSPLITTER+"d12", llcse);
		redis.put("F"+Constants.QSPLITTER+"cff"+Constants.QSPLITTER+"f11"
				+Constants.QSPLITTER+"cc5", llcsf);
		MapJoinRelationList mrlist=new MapJoinRelationList();
		MapJoinHbaseImplementation m=new MapJoinHbaseImplementation();
		System.out.println(m.filter(jsc, lcs2, mapJoin, redis));
		m.genList(mrlist,mapJoin);
		ListCacadeCell ll=new ListCacadeCell();
		m.genResult(mrlist,ll);
		for(Map<JoinRelationShip, String> mm:ll.getCascadeCellList()){
			StringBuilder sb=new StringBuilder("");
			for(JoinRelationShip js:mm.keySet()){
				sb.append(js.getTableName()+"."+js.getJoinPoint()+":"+mm.get(js)+",");
			}
			System.out.println(sb.toString());
		}
		System.out.println();
	}

	public static OmsJedisCluster getJedis() {
		return jedis;
	}
}