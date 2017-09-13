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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType.Operation;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.StringList;
import org.apache.hadoop.hbase.coprocessor.observer.handler.IndexRegionObserverPutHandler;
import org.apache.hadoop.hbase.coprocessor.observer.handler.IndexRegionObserverUpdateHandler;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ComplexRowFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("deprecation")
public class IndexRegionObserverUpHandler extends IndexRegionObserverPutHandler{
	protected static final Log logger = LogFactory
			.getLog(IndexRegionObserverUpHandler.class);
	private static IndexRegionObserverUpHandler instance=new IndexRegionObserverUpHandler();
	private IndexRegionObserverUpHandler(){
	}
	public static IndexRegionObserverUpHandler getInstance(){
		return instance;
	}
	@Override
	public void handleRegionRow(Put put, String tbName,Map<String, Cell> mapk,String familyName,Region region,Map<StringList, MetaIndex> multiIndexMap){
		List<Cell> l = put.getFamilyCellMap().values().iterator().next();
		StringBuilder sb = null;
		Result r=fetchpreUpdatePut(put.getRow(),region);
			if(r==null||r.isEmpty()){
				put.setAttribute(Constants.HBASEERRORUPDATEROW4DEL, put.getRow());
				return;
			}
			sb =  new StringBuilder(".*(");
		Map<Cell,Set<MetaIndex>> delMap=new HashMap<Cell,Set<MetaIndex>>();
		for (String kk : mapk.keySet()) {
			Set<MetaIndex> setMeIndx=delMap.get(mapk.get(kk));
			if(setMeIndx==null){
				setMeIndx=new HashSet<MetaIndex>();
			}
			for (StringList k : multiIndexMap.keySet()) {
				if (k.contain(kk)){
					setMeIndx.add(multiIndexMap.get(k));
				}
			}
			delMap.put(mapk.get(kk), setMeIndx);
		}
		addDelIdxList(sb, delMap,familyName,r);
		if(!".*(".equals(sb.toString())){
			String rexStr = sb.substring(0, sb.length() - 1);
			rexStr += ").*";
			handleUpdateDelIdx(familyName,region, rexStr,l,Bytes.toString(put.getRow()),multiIndexMap);
		}
		handleUpdateDelRow(l,familyName,put,region);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public Map<String,List> handleIdxRow(String tbName,Put put,Region region,Map<String, Cell> mapk,String familyName,RowKeyComposition rkc){
		Map<String,List> map=new HashMap<String,List>(2);
		try {
			List<EsIdxHbaseType> updateList=new ArrayList<EsIdxHbaseType>();
//			List<Delete> delList=new ArrayList<Delete>();
//			Get gg=new Get(put.getRow());
			String rowPrefix=buildRowPrefix(put);
			for(String q:mapk.keySet()){
				if(rkc.getFamilyColsNeedIdx().contains(q)){
					String v=Bytes.toString(mapk.get(q).getValue());
					if(rkc.getMainHbaseCfPk().getOidQulifierName().equals(q))
						v=rkc.getMainHbaseCfPk().buidFixedLengthNumber(v);
//					Put p=new Put(new StringBuilder(q).append(Constants.QLIFIERSPLITTER).append(v)
//							.append(Constants.REGDELHBASEPREFIIX).append(rowPrefix).toString().getBytes()); 
//					p.addColumn(familyName.getBytes(), Constants.IDXPREFIX.getBytes(), "".getBytes());
					EsIdxHbaseType p=new EsIdxHbaseType();
					p.setColumnName(q);
					p.setFamilyName(familyName);
					p.setIdxValue(v);
					p.setOp(Operation.Update);
					p.setPrefix(rowPrefix);
					p.setRowKey(Bytes.toString(put.getRow()));
					p.setMainColumn(rkc.getMainHbaseCfPk().getOidQulifierName());
					p.setTbName(tbName);
					updateList.add(p);
//					gg.addColumn(familyName.getBytes(), q.getBytes());
				}
			}
			if(updateList.isEmpty())
				return map;
//			Result r = region.get(gg);
//			if(r==null||r.isEmpty())
//				return map;
//			for(Cell c:r.listCells()){
//				Delete del=new Delete(new StringBuilder(Bytes.toString(c.getQualifier())).append(Constants.QLIFIERSPLITTER).
//						append(Bytes.toString(c.getValue())).append(Constants.REGDELHBASEPREFIIX)
//						.append(rowPrefix).toString().getBytes());
//				del.addFamily(c.getFamily());
//				delList.add(del);
//			}
			map.put(Constants.HBASEUPDATEROWFORUPDATE,updateList);
//			map.put(Constants.HBASEUPDATEROWFORDEL,delList);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
		return map;
	}
	
	private String buildRowPrefix(Put put) {
		String[] rr=Bytes.toString(put.getRow()).split(Constants.SPLITTER);
		return rr[0];
	}
	
	private void addDelIdxList(StringBuilder sb,
			Map<Cell, Set<MetaIndex>> delMap, String familyName, Result r) {
		for(Cell c:delMap.keySet()){
			Set<MetaIndex> set=delMap.get(c);
			for(MetaIndex mi:set){
				addDelIdxList(sb, mi, c,
						familyName,r);
			}
		}
		
	}
	
	
	private void addDelIdxList(StringBuilder sb, MetaIndex mi,
			Cell c, String familyName, Result r) {
		if (sb == null)
			return ;
		Cell cc=getOrgCC(r,c);
		String[] ss = Bytes.toString(c.getRow())
				.split(Constants.SPLITTER);
		sb.append(ss[0])
				.append(Constants.REGDELHBASEPREFIIX).append(mi.getIdxName())
				.append(Constants.SPLITTER).append(Constants.REGSPLITER)
				.append(Bytes.toString(cc.getQualifier())).append(Constants.QSPLITTER)
					.append(Bytes.toString(cc.getValue())).append(Constants.REGSPLITER)
					.append(Constants.SPLITTER);
		sb.append(ss[mi.getDestPos()]).append(Constants.REGSPLITTER);
	}
	
	private Cell getOrgCC(Result r,Cell c) {
		for(Cell cc:r.listCells()){
			if(Bytes.toString(cc.getQualifier()).equals(Bytes.toString(c.getQualifier()))){
				return cc;
			}
		}
		return null;
	}
	
	private void handleUpdateDelIdx(String familyName,
			Region region, String rexStr,List<Cell> l, String rowS,Map<StringList, MetaIndex> multiIndexMap) {
			Scan scan = new Scan();
			String[] rr=rowS.split(Constants.SPLITTER);
			scan.setStartRow(new StringBuilder(rr[0]).append(Constants.REGIDXHBASESTART).toString().getBytes());
			scan.setStopRow(new StringBuilder(rr[0]).append(Constants.REGIDXHBASESTOP).toString().getBytes());
			if (!StringUtils.isBlank(familyName))
				scan.addColumn(familyName.getBytes(),
						Constants.IDXPREFIX.getBytes());
			ComplexRowFilter filter1 = new ComplexRowFilter(CompareFilter.CompareOp.EQUAL,
					new RegexStringComparator(rexStr.toString()));
			scan.setCaching(25);
			scan.setFilter(filter1);
			try {
				InternalScanner is = region.getScanner(scan);
				boolean hasMoreRows = false;
				List<Delete> listDelIdx=new ArrayList<Delete>();
				List<Put> listPutIdx=new ArrayList<Put>();
				do {
					List<Cell> cells = new ArrayList<Cell>();
					hasMoreRows = is.next(cells);
					if (!cells.isEmpty()) {
						String row=Bytes.toString(cells.get(0).getRow());
						String rowPut=buildRowPut(row,l,region.getTableDesc().getTableName().getNameAsString(),familyName,multiIndexMap);
						Delete del = new Delete(row.getBytes());
						Put p=new Put(rowPut.getBytes());
						p.addColumn(familyName.getBytes(), Constants.IDXPREFIX.getBytes(),
								"".getBytes());
						p.setAttribute(Constants.IDXHBASEPUT, "".getBytes());
						p.setDurability(Durability.ASYNC_WAL);
						listPutIdx.add(p);
						del.setDurability(Durability.ASYNC_WAL);
						del.setAttribute(Constants.IDXHBASEDEL, "".getBytes());
						listDelIdx.add(del);
					}
				} while (hasMoreRows);
				is.close();
				if(!listDelIdx.isEmpty()){
					HRegion h=(HRegion) region;
					h.batchMutate(listDelIdx.toArray(new Delete[]{}));
					h.batchMutate(listPutIdx.toArray(new Put[]{}));
				}
			} catch (IOException e) {
				logger.error(e.getMessage(),e);
			}
	}
	
	private void handleUpdateDelRow(List<Cell> l, String familyName, Put put, Region region) {
			String sw = IndexRegionObserverUpdateHandler.getInstance()
					.buildUpdateRowPut(Bytes.toString(put.getRow()), l,
							familyName, region.getTableDesc());
			if (!StringUtils.isBlank(sw)) {
				put.setAttribute(Constants.HBASEUPDATEROWFORDEL,
						sw.getBytes());
			}
	}
	
	private String buildRowPut(String row,List<Cell> l,String tbName, String familyName,Map<StringList, MetaIndex> multiIndexMap){
		return IndexRegionObserverUpdateHandler.getInstance().buildUpdateRowIdxPut(row, l, tbName, familyName, multiIndexMap);
	}
}