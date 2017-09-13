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
package org.apache.hadoop.hbase.client.coprocessor.model;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CasCadeListCell;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CascadeCell;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;

import redis.clients.jedis.JedisCluster;

import com.alibaba.fastjson.JSON;

public class TableDmlModel {
//	private static final Logger logger = LoggerFactory.getLogger(TableDmlModel.class);
	public TableDmlModel(){
	}
	private String prefix;
	private Map<String,String> desc;
	public Map<String,String> getDesc() {
		return desc;
	}

	public void setDesc(Map<String,String> desc) {
		this.desc = desc;
	}

	public CasCadeListCell getMapCell() {
		return mapCell;
	}

	public void setMapCell(CasCadeListCell mapCell) {
		this.mapCell = mapCell;
	}

	public Map<String, String> getQulifierAndValue() {
		return qulifierAndValue;
	}

	public void setQulifierAndValue(Map<String, String> qulifierAndValue) {
		this.qulifierAndValue = qulifierAndValue;
	}

	public String getFamiliName() {
		return familiName;
	}

	public void setFamiliName(String familiName) {
		this.familiName = familiName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	private Map<String, String> qulifierAndValue=new HashMap<String, String>();
	private String familiName;
	private String tableName;
	
	private CasCadeListCell mapCell;
	public final String buildRowKey(JedisCluster jedis,String... prefix) {
		if(!StringUtils.isBlank(this.prefix))
			return build(this.prefix);
		if(prefix==null||prefix.length==0){
		String kk =jedis==null?buildRandomPrefix():buildSequencePrefix(jedis); 
		return build(kk);
		}
		return build(prefix[0]);
	}

	@SuppressWarnings({"unchecked" })
	public String buildSequencePrefix(JedisCluster jedis) {
		if(desc==null)
			throw new IllegalStateException("lack of meta Table data:"+tableName); 
		String regionNum=desc.get(Constants.HBASEREGIONNUM);
		Map<String,String> regionMap=JSON.parseObject(desc.get(Constants.HBASEJEDISTABLEMAP), Map.class);
		String kk=generateSequencePrefixRowKey(regionNum==null?1:Integer.parseInt(regionNum),regionMap,jedis);
		if(kk==null)
			throw new IllegalStateException("lack of meta Table data:"+tableName);
		return kk;
	}
	
	private String generateSequencePrefixRowKey(int length, Map<String, String> regionMap, JedisCluster jedis) {
		if(length>36||length<1)
			throw new IllegalStateException(tableName+"'s pre region num can not gt 36 or le 1"); 
		String x=Long.toString(Math.abs(UUID.randomUUID().getMostSignificantBits() % length)+1,36);
		StringBuilder sb=new StringBuilder("10".equals(x)?"0":x);
		String hkey=regionMap.get(sb.toString());
		if(hkey==null)
			hkey=regionMap.get("");
		Long num=jedis.hincrBy(tableName, hkey, 1);
		String ssnum="";
		try {
			ssnum = HbaseUtil.addZeroForNum(Long.toString(num,36), 8);
		} catch (Exception e) {
			e.printStackTrace();
		}
		sb.append(ssnum);
		return sb.toString();
	}
	
	public String buildRandomPrefix() {
		if(desc==null)
			throw new IllegalStateException("lack of meta Table data:"+tableName); 
		String regionNum=desc.get(Constants.HBASEREGIONNUM);
		String kk=generateDiscretePrefixRowKey(regionNum==null?1:Integer.parseInt(regionNum));
		if(kk==null)
			throw new IllegalStateException("lack of meta Table data:"+tableName);
		return kk;
	}
	
	private String generateDiscretePrefixRowKey(int length) {
		if(length>36||length<1)
			throw new IllegalStateException(tableName+"'s pre region num can not gt 36 or le 1"); 
		StringBuilder sb=new StringBuilder(Long.toString(Math.abs(UUID.randomUUID().getMostSignificantBits() % length),36));
		sb.append(Long.toString(Math.abs((int)UUID.randomUUID().getMostSignificantBits()),36));
		return sb.toString();
	}

	private final String build(String kk) {
		StringBuilder sb=new StringBuilder(kk).append(Constants.REGTABLEHBASEPREFIIX);
		RowKeyComposition rkc=JSON.parseObject(desc.get(familiName),RowKeyComposition.class);
		int pos=rkc.buildOidPos()+2;
		if(rkc.getOrderBy()!=null){
			if(!qulifierAndValue.containsKey(rkc.getOrderBy().getQulifier()))
				throw new IllegalStateException("Col Value Map must contain:"+rkc.getOrderBy().getQulifier()); 
			String orderByValue=rkc.getOrderBy().build(qulifierAndValue.get(rkc.getOrderBy().getQulifier()));
			if(orderByValue!=null)
				sb.//append(rkc.getOrderBy().getQulifier()).append(Constants.QSPLITTERORDERBY).
				append(orderByValue).append(Constants.QLIFIERSPLITTER);
		}
		TreeSet<String> colKeysForRow=rkc.getFamilyColsForRowKey();
		int i=0;
		for(String q:colKeysForRow){
			String v=qulifierAndValue.get(q);
			if(v==null) throw new IllegalStateException("Col Value Map must contain:"+q); 
			if(rkc.getMainHbaseCfPk().getOidQulifierName().equals(q))
				v=rkc.getMainHbaseCfPk().buidFixedLengthNumber(v);
			sb.append(q).append(Constants.QSPLITTER).append(v);
			if(i++<colKeysForRow.size()-1)
				sb.append(Constants.SPLITTER);
		}
		if (mapCell != null && mapCell.getListCell()!=null) {
			for (String k : mapCell.getListCell().keySet()) {
				for (CascadeCell cell : mapCell.getListCell().get(k)) {
					cell.buildCasCadeRowKey(sb.toString(), pos, desc);
				}
			}
		}
		return sb.toString();
	}
	
	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
}