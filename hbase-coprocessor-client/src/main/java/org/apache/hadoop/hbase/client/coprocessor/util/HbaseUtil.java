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
package org.apache.hadoop.hbase.client.coprocessor.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbasePutException;
import org.apache.hadoop.hbase.client.coprocessor.model.ScanResult;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaFamilyIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaFamilyPosIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaTableIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CasCadeScanMap;
import org.apache.hadoop.hbase.client.coprocessor.util.kryo.KryoObjectInput;
import org.apache.hadoop.hbase.client.coprocessor.util.kryo.KryoObjectOutput;
import org.apache.hadoop.hbase.io.util.HeapMemorySizeUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.elasticsearch.client.support.AbstractClient;

import com.alibaba.fastjson.JSON;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.lppz.configuration.es.EsBaseYamlBean;
import com.lppz.configuration.es.EsClusterPool;
import com.lppz.elasticsearch.EsClientUtil;

/**
 *
 */
@SuppressWarnings("deprecation")
public class HbaseUtil {
	private static final Log logger = LogFactory.getLog(HbaseUtil.class);

	public static Map<String, Cell> buildMultiCell(List<Cell> k) {
		Map<String, Cell> map = new HashMap<String, Cell>();
		for (Cell kk : k) {
			map.put(Bytes.toString(kk.getQualifier()), kk);
		}
		return map;
	}

	public static Scan cloneScan(Scan scan) {
		if (scan == null)
			return null;
		Scan s = new Scan();
		s.setReversed(scan.isReversed());
		for (byte[] family : scan.getFamilyMap().keySet()) {
			s.addFamily(family);
			Set<byte[]> cols = scan.getFamilyMap().get(family);
			if (cols != null) {
				for (byte[] qualifier : cols) {
					s.addColumn(family, qualifier);
				}
			}
		}
		s.setFilter(scan.getFilter());
		s.setCaching(scan.getCaching());
		return s;
	}

	public static void checkprePutValid(Put put) throws HbasePutException {
		if (put.getFamilyCellMap() != null) {
			for (Iterator<List<Cell>> it = put.getFamilyCellMap().values()
					.iterator(); it.hasNext();) {
				List<Cell> l = it.next();
				for (Cell c : l) {
					String q = Bytes.toString(c.getQualifier());
					String v = Bytes.toString(c.getValue());
					if (q.contains(Constants.SPLITTER)
							|| q.contains(Constants.QSPLITTER)
							|| q.contains(Constants.REGSPLITTER)
							|| q.contains(Constants.IDXPREFIX)
							|| q.contains(Constants.QLIFIERSPLITTER))
						throw new HbasePutException("qulifier:" + q
								+ " can not contain " + Constants.SPLITTER
								+ " or " + Constants.QSPLITTER + " or "
								+ Constants.IDXPREFIX + " or "
								+ Constants.QLIFIERSPLITTER + " or "
								+ Constants.REGSPLITTER);
					if (v.contains(Constants.SPLITTER)
							|| v.contains(Constants.QSPLITTER)
							|| v.contains(Constants.REGSPLITTER)
							|| v.contains(Constants.QLIFIERSPLITTER))
						throw new HbasePutException("value:" + v
								+ " can not contain " + Constants.SPLITTER
								+ " or " + Constants.QSPLITTER + " or "
								+ Constants.QLIFIERSPLITTER + " or "
								+ Constants.REGSPLITTER);
				}
			}
		}
	}

	public static Map<String, Double> calcSum(List<Map<String, Double>> list) {
		Map<String, Double> map = new HashMap<String, Double>();
		for (Map<String, Double> m : list) {
			if (m == null)
				throw new IllegalStateException("sum can not be null");
			for (String ms : m.keySet()) {
				Double d = map.get(ms);
				if (d == null) {
					d = 0d;
				}
				map.put(ms,
						BigDecimal.valueOf(d)
								.add(BigDecimal.valueOf(m.get(ms)))
								.doubleValue());
			}
		}
		return map;
	}

	public static <T> Map<String, T> calcMax(List<Map<String, T>> list,
			Class<T> clazz) {
		Map<String, T> map = new HashMap<String, T>();
		for (Map<String, T> m : list) {
			if (m == null)
				throw new IllegalStateException("max can not be null");
			for (String ms : m.keySet()) {
				T d = map.get(ms);
				if (clazz == Double.class) {
					Double dd = (Double) d;
					if (d == null) {
						map.put(ms, m.get(ms));
						continue;
					}
					map.put(ms, dd >= (Double) m.get(ms) ? d : m.get(ms));
				} else if (clazz == String.class) {
					String dd = (String) d;
					if (d == null) {
						map.put(ms, m.get(ms));
						continue;
					}
					map.put(ms,
							dd.compareTo((String) m.get(ms)) >= 0 ? d : m
									.get(ms));
				} else if (clazz == Date.class) {
					Date dd = (Date) d;
					if (d == null) {
						map.put(ms, m.get(ms));
						continue;
					}
					map.put(ms,
							dd.compareTo((Date) m.get(ms)) >= 0 ? d : m.get(ms));
				}
			}
		}
		return map;
	}

	public static Map<String, Integer> calcCount(List<Map<String, Integer>> list) {
		Map<String, Integer> map = new HashMap<String, Integer>();
		for (Map<String, Integer> m : list) {
			if (m == null)
				throw new IllegalStateException("count can not be null");
			for (String ms : m.keySet()) {
				Integer d = map.get(ms);
				if (d == null) {
					d = 0;
				}
				map.put(ms, d + m.get(ms));
			}
		}
		return map;
	}

	public static <T> Map<String, T> calcMin(List<Map<String, T>> list,
			Class<T> clazz) {
		Map<String, T> map = new HashMap<String, T>();
		for (Map<String, T> m : list) {
			if (m == null)
				throw new IllegalStateException("min can not be null");
			for (String ms : m.keySet()) {
				T d = map.get(ms);
				if (clazz == Double.class) {
					Double dd = (Double) d;
					if (d == null) {
						map.put(ms, m.get(ms));
						continue;
					}
					map.put(ms, dd <= (Double) m.get(ms) ? d : m.get(ms));
				} else if (clazz == String.class) {
					String dd = (String) d;
					if (d == null) {
						map.put(ms, m.get(ms));
						continue;
					}
					map.put(ms,
							dd.compareTo((String) m.get(ms)) <= 0 ? d : m
									.get(ms));
				} else if (clazz == Date.class) {
					Date dd = (Date) d;
					if (d == null) {
						map.put(ms, m.get(ms));
						continue;
					}
					map.put(ms,
							dd.compareTo((Date) m.get(ms)) <= 0 ? d : m.get(ms));
				}
			}
		}
		return map;
	}

	public static <T> byte[] kyroSeriLize(T t, int maxBufferSize)
			throws IOException {
		Output output = new Output(1, maxBufferSize);
		KryoObjectOutput ko = new KryoObjectOutput(output);
		try {
			ko.writeObject(t);
			ko.flushBuffer();
			byte[] bb = output.toBytes();
			return bb;
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
			throw e;
		} finally {
			ko.cleanup();
		}
	}

	public static <T> T kyroDeSeriLize(byte[] bb, Class<T> clazz)
			throws Exception {
		Input input = new Input(bb);
		KryoObjectInput ko = new KryoObjectInput(input);
		try {
			T t = ko.readObject(clazz);
			return t;
		} catch (IOException | ClassNotFoundException e) {
			logger.error(e.getMessage(), e);
			throw e;
		} finally {
			ko.cleanup();
		}
	}

	public static void zkRmr(RecoverableZooKeeper rz, String parentPath)
			throws KeeperException, InterruptedException,
			UnsupportedEncodingException {
		List<String> znodes = rz.getChildren(parentPath, false);
		if (znodes != null && !znodes.isEmpty()) {
			for (String znode : znodes) {
				rz.delete(parentPath + "/" + znode, -1);
			}
		}
		rz.delete(parentPath, -1);
	}

	public static MetaTableIndex initMetaIndex(RecoverableZooKeeper rz)
			throws KeeperException, InterruptedException,
			UnsupportedEncodingException {
		initRzIdxPath(rz);
		MetaTableIndex metaIndex = new MetaTableIndex();
		initMetaIndexMap(rz, metaIndex);
		initMetaIdxNameMap(rz, metaIndex);
		return metaIndex;
	}

	private static void initRzIdxPath(RecoverableZooKeeper rz)
			throws KeeperException, InterruptedException {
		if (rz.exists(Constants.ZOOINDEXPATH, true) == null) {
			rz.create(Constants.ZOOINDEXPATH, null, Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
			if (rz.exists(Constants.ZOOINDEXPATH
					+ Constants.ZOOINDEXTBINDEXMAPPATH, true) == null) {
				rz.create(Constants.ZOOINDEXPATH
						+ Constants.ZOOINDEXTBINDEXMAPPATH, null,
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			if (rz.exists(Constants.ZOOINDEXPATH
					+ Constants.ZOOINDEXTBINDEXNAMEMAPPATH, true) == null) {
				rz.create(Constants.ZOOINDEXPATH
						+ Constants.ZOOINDEXTBINDEXNAMEMAPPATH, null,
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		}
	}

	private static void initMetaIdxNameMap(RecoverableZooKeeper rz,
			MetaTableIndex metaIndex) throws KeeperException,
			InterruptedException, UnsupportedEncodingException {
		List<String> list = rz.getChildren(Constants.ZOOINDEXPATH
				+ Constants.ZOOINDEXTBINDEXNAMEMAPPATH, false);
		if (list != null && !list.isEmpty()) {
			for (String tbName : list) {
				Map<String, MetaFamilyPosIndex> mmi = new HashMap<String, MetaFamilyPosIndex>();
				List<String> familiList = rz.getChildren(Constants.ZOOINDEXPATH
						+ Constants.ZOOINDEXTBINDEXNAMEMAPPATH + "/" + tbName,
						false);
				if (familiList != null && !familiList.isEmpty()) {
					for (String family : familiList) {
						byte[] b = rz.getData(Constants.ZOOINDEXPATH
								+ Constants.ZOOINDEXTBINDEXNAMEMAPPATH + "/"
								+ tbName + "/" + family, true, null);
						if (b != null) {
							String str = new String(b, "utf-8");
							MetaFamilyPosIndex mfi = JSON.parseObject(str,
									MetaFamilyPosIndex.class);
							mmi.put(family, mfi);
						}
					}
				}
				metaIndex.getTbIndexNameMap().put(tbName, mmi);
			}
		}
	}

	private static void initMetaIndexMap(RecoverableZooKeeper rz,
			MetaTableIndex metaIndex) throws KeeperException,
			InterruptedException, UnsupportedEncodingException {
		List<String> list = rz.getChildren(Constants.ZOOINDEXPATH
				+ Constants.ZOOINDEXTBINDEXMAPPATH, false);
		if (list != null && !list.isEmpty()) {
			for (String tbName : list) {
				Map<String, MetaFamilyIndex> mmi = new HashMap<String, MetaFamilyIndex>();
				List<String> familiList = rz.getChildren(Constants.ZOOINDEXPATH
						+ Constants.ZOOINDEXTBINDEXMAPPATH + "/" + tbName,
						false);
				if (familiList != null && !familiList.isEmpty()) {
					for (String family : familiList) {
						byte[] b = rz.getData(Constants.ZOOINDEXPATH
								+ Constants.ZOOINDEXTBINDEXMAPPATH + "/"
								+ tbName + "/" + family, true, null);
						if (b != null) {
							String str = new String(b, "utf-8");
							MetaFamilyIndex mfi = JSON.parseObject(str,
									MetaFamilyIndex.class);
							mmi.put(family, mfi);
						}
					}
				}
				metaIndex.getTbIndexMap().put(tbName, mmi);
			}
		}
	}

	public static String addZeroForNum(String str, int strLength) {
		int strLen = str.length();
		if (strLen < strLength) {
			while (strLen < strLength) {
				StringBuilder sb = new StringBuilder();
				sb.append("0").append(str);
				str = sb.toString();
				strLen = str.length();
			}
		}
		return str;
	}

	public static Set<List<String>> buildSet(Set<String> set,boolean isPager) {
		Set<List<String>> llset = new LinkedHashSet<List<String>>();
		List<String> ll = new ArrayList<String>();
		for (String s : set) {
			if (ll.isEmpty()) {
				ll.add(s);
				continue;
			}
			String x = ll.get(ll.size() - 1);
			if (compare(x, s)&&!isPager) {
				ll.add(s);
				continue;
			}
			llset.add(ll);
			ll = new ArrayList<String>();
			ll.add(s);
		}
		llset.add(ll);
		return llset;
	}

	public static boolean compare(String x, String s) {
		if (s.length() != x.length())
			return false;
		Long ss = Long.parseLong(s.substring(1), 36);
		Long xx = Long.parseLong(x.substring(1), 36);
		return ss - xx <= 100;
	}

	public static List<ScanResult> scanNormal(String tableName,
			CasCadeScanMap casCadeScanMap, HTableInterface table, Scan scan)
			throws Exception, IOException {
		ResultScanner rs = null;
		try {
			rs = table.getScanner(scan);
			List<ScanResult> lscan = new ArrayList<ScanResult>();
			for (Result r : rs) {
				lscan.add(new ScanResult(r, casCadeScanMap == null));
			}
			return lscan;
		} catch (Exception e) {
			throw e;
		} finally {
			if (rs != null)
				rs.close();
		}
	}

	public static AbstractClient buildEsClient(Configuration cf) {
		EsBaseYamlBean tmpjedisObj = new EsBaseYamlBean();
		tmpjedisObj.setClusterName(cf.get(Constants.ES.clustername));
		String stringNode = cf.get(Constants.ES.clusternodestring);
		List<Properties> esclusterNode = new ArrayList<Properties>();
		for (String ss : stringNode.split(",")) {
			Properties p = new Properties();
			String[] s = ss.split(":");
			p.setProperty("host", s[0]);
			p.setProperty("port", s[1]);
			esclusterNode.add(p);
		}
		tmpjedisObj.setEsclusterNode(esclusterNode);
		EsClusterPool esClusterPool = new EsClusterPool();
		esClusterPool.setMaxIdle(cf.get(Constants.ES.maxidle));
		esClusterPool.setMaxTotal(cf.get(Constants.ES.maxtotal));
		esClusterPool.setMaxWaitMillis(cf.get(Constants.ES.maxwaitmillis));
		tmpjedisObj.setEsClusterPool(esClusterPool);
		return EsClientUtil.buildPoolClientProxy(tmpjedisObj);
	}

	public static boolean checkIsUpdatePut(Put put) {
		return put.getAttribute(Constants.HBASEUPDATEROW) != null;
	}

	public static boolean checkRkcIsUpdateMainCol(Put put,
			RowKeyComposition rkc, Map<String, Cell> mapk) {
		if (checkIsUpdatePut(put)) {
			for (String q : mapk.keySet()) {
				if (rkc.getMainHbaseCfPk().getOidQulifierName().equals(q))
					return true;
			}
		}
		return false;
	}

	public static Map<String, String> buildConMapByRowKey(String row) {
		String[] ss = null;
		if (row.contains(Constants.QLIFIERSPLITTER)) {
			String r = row.split(Constants.QLIFIERSPLITTER, 2)[1];
			ss = r.split(Constants.SPLITTER);
		} else
			ss = row.split(Constants.SPLITTER);
		Map<String, String> conmap = new HashMap<String, String>(ss.length);
		for (String s : ss) {
			if (s.contains(Constants.QSPLITTER)) {
				String[] k = s.split(Constants.QSPLITTER);
				conmap.put(k[0], k.length == 1 ? "NULL" : k[1]);
			}
		}
		return conmap;
	}

	public static Configuration createConf(String clientPort, String quorum,
			Long timeout, Integer period, Long caching) {
		Configuration conf = new Configuration();
		conf.setClassLoader(HBaseConfiguration.class.getClassLoader());
		conf.addResource("hbase-default.xml");
		conf.addResource("hbase-site.xml");
		HeapMemorySizeUtil.checkForClusterFreeMemoryLimit(conf);
		conf.set("hbase.zookeeper.property.clientPort", clientPort);
		conf.set("hbase.zookeeper.quorum", quorum);
		conf.setLong("hbase.rpc.timeout", timeout == null ? 30000 : timeout);
		conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
				period == null ? 60000 : period);
		conf.set("hbase.client.pause", "50");
		conf.set("hbase.client.retries.number", "5");
		conf.setLong("hbase.client.scanner.caching", caching == null ? 20
				: caching);
		return conf;
	}
	
	public static String buildEsIdx(String hbaseTbName,String hbaseFamily,String column,String tenant){
		StringBuilder sb=new StringBuilder(Constants.IDXTABLENAMEESPREFIX);
			sb.append(hbaseTbName).append("-").append(hbaseFamily).append("-").append(column);
		if(StringUtils.isBlank(tenant))
			tenant="2017";
//			tenant=new SimpleDateFormat("yyyy").format(new Date());
		sb.append("-").append(tenant);
		return sb.toString();
	}
	
	public static String buildPrefixByMainId(String mainId){
		if(StringUtils.isBlank(mainId))
			return null;
		int radix=36;
		String x=Long.toString(Math.abs(mainId.hashCode()%radix),radix);
		StringBuilder sb=new StringBuilder("10".equals(x)?"0":x);
		sb.append(Long.toString(Math.abs(mainId.hashCode()),radix));
		return sb.toString();
	}
	
	public static void main(String[] args) {
		String mainId="u1021224";
		for(int i=0;i<10000;i++){
			String x=Long.toString(Math.abs(mainId.hashCode()%36),36);
			System.out.println(x);
		}
	}
}