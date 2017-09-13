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
package org.apache.hadoop.hbase.coprocessor.observer.handler.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType.Operation;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.HbaseDataType;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.StringList;
import org.apache.hadoop.hbase.coprocessor.observer.handler.IndexRegionObserverPutHandler;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("deprecation")
public class IndexRegionObserverInsertHandler extends IndexRegionObserverPutHandler{
	protected static final Log logger = LogFactory
			.getLog(IndexRegionObserverInsertHandler.class);
	private static IndexRegionObserverInsertHandler instance=new IndexRegionObserverInsertHandler();
	private IndexRegionObserverInsertHandler(){
	}
	public static IndexRegionObserverInsertHandler getInstance(){
		return instance;
	}
	@Override
	public void handleRegionRow(Put put, String tbName,Map<String, Cell> mapk,String familyName,Region region,Map<StringList, MetaIndex> multiIndexMap){
		if (multiIndexMap != null) {
			List<Put> listIdx = new ArrayList<Put>();
			a: for (StringList k : multiIndexMap.keySet()) {
				Map<String, Cell> la = new TreeMap<String, Cell>();
				StringBuilder ssb=new StringBuilder("");
				MetaIndex mi = multiIndexMap.get(k);
				if(k.getHbasedataList()!=null){
					int i=0;
				for(HbaseDataType hdt:k.getHbasedataList()){
					if (!mapk.containsKey(hdt.getQulifier()))
						continue a;
					String v=hdt.build(Bytes.toString(mapk.get(hdt.getQulifier()).getValue()));
					if(v!=null)
					ssb.append(v);
					la.put(hdt.getQulifier(), mapk.get(hdt.getQulifier()));
					if(i++==k.getHbasedataList().size()-1)
						ssb.append(Constants.QLIFIERSPLITTER);
					else
						ssb.append(Constants.QSPLITTERORDERBY);
				}
				}
				for (String kk : k.getQulifierList()) {
					if (!mapk.containsKey(kk))
						continue a;
					la.put(kk, mapk.get(kk));
				}
				Put pp=buildIdx(mi, la, familyName,ssb,region);
				pp.setDurability(Durability.ASYNC_WAL);
				listIdx.add(pp);
			}
			if (listIdx.isEmpty()) {
				return;
			}
			HRegion h = (HRegion) region;
			try {
				h.batchMutate(listIdx.toArray(new Put[]{}));
			} catch (IOException e) {
				logger.error(e.getMessage(),e);
			}
		}
	}
	
	private List<EsIdxHbaseType> buildListPut(String tbName,String rowKey,String rowPrefix,String familyName, Map<String, Cell> mapk,RowKeyComposition rkc) {
		if(rkc.getFamilyColsNeedIdx()==null)
			return null;
		List<EsIdxHbaseType> listPut=new ArrayList<EsIdxHbaseType>();
		for(String s:mapk.keySet()){
			if(rkc.getFamilyColsNeedIdx().contains(s)){
				Cell c=mapk.get(s);
				String v=Bytes.toString(c.getValue());
				EsIdxHbaseType p=new EsIdxHbaseType();
				if(rkc.getMainHbaseCfPk().getOidQulifierName().equals(s)){
					v=rkc.getMainHbaseCfPk().buidFixedLengthNumber(v);
				}
//				Put p=new Put(new StringBuilder(s).append(Constants.QLIFIERSPLITTER).
//						append(v).append(Constants.REGDELHBASEPREFIIX).append(rowPrefix).toString().getBytes());
//				p.addColumn(familyName.getBytes(),Constants.IDXPREFIX.getBytes(),"".getBytes());
//				p.setAttribute(Constants.IDXHBASEPUT, "".getBytes());
				p.setColumnName(s);
				p.setFamilyName(familyName);
				p.setIdxValue(v);
				p.setOp(Operation.Insert);
				p.setPrefix(rowPrefix);
				p.setRowKey(rowKey);
				p.setMainColumn(rkc.getMainHbaseCfPk().getOidQulifierName());
				p.setTbName(tbName);
				listPut.add(p);
			}
		}
		return listPut;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public Map<String,List> handleIdxRow(String tbName,Put put,Region region,Map<String, Cell> mapk,String familyName,RowKeyComposition rkc){
		String rowPrefix=buildRowPrefix(Bytes.toString(put.getRow()));
		List<EsIdxHbaseType> listIdx=buildListPut(tbName,Bytes.toString(put.getRow()),rowPrefix, familyName, mapk, rkc);
		Map<String,List> map=new HashMap<String,List>(1);
		if(listIdx!=null)
			map.put(Constants.HBASEUPDATEROWFORUPDATE, listIdx);
		return map;
	}
	
	private String buildRowPrefix(String row) {
		return row.split(Constants.SPLITTER)[0];
	}
	
	private Put buildIdx(MetaIndex mi, Map<String, Cell> la, String familyName, StringBuilder ssb, Region region) {
		String[] ss = Bytes.toString(la.values().iterator().next().getRow())
				.split(Constants.SPLITTER);
		StringBuilder sb = new StringBuilder(ss[0]);
		sb.append(Constants.SPLITTER).append("i").append(Constants.SPLITTER)
				.append(mi.getIdxName()).append(Constants.SPLITTER).append(ssb);
		for (String q : la.keySet()) {
			sb.append(q).append(Constants.QSPLITTER)
					.append(Bytes.toString(la.get(q).getValue()))
					.append(Constants.SPLITTER);
		}
		sb.append(ss[mi.getDestPos()]);
		if(!familyName.equals(region.getTableDesc().getValue(Constants.HBASEMAINTABLE)))
			sb.append(Constants.SPLITTER).append(ss[ss.length-1]);
		Put put = new Put(Bytes.toBytes(sb.toString()));
		put.addColumn(familyName.getBytes(), Constants.IDXPREFIX.getBytes(),
				"".getBytes());
		put.setAttribute(Constants.IDXHBASEPUT, "".getBytes());
		return put;
	}
}