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
package org.apache.hadoop.hbase.client.coprocessor.model.scan;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.MultiIndexFetcher;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbaseScanException;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.HbaseDataType;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.ListScanOrderedKV;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaFamilyIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaScan;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaTableIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.ScanOrderStgKV;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.ComplexRowFilter;
import org.apache.hadoop.hbase.filter.ComplexSubstringComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ConcurrentHashMultiset;

public class PartRowKeyString implements Serializable{
	/**
	 * 
	 */
	private static final Log LOG = LogFactory.getLog(PartRowKeyString.class
			.getName());
	private static final long serialVersionUID = -5473997551531562498L;
	private List<TreeMap<String,String>> complexScanCond;
	private List<ScanOrderedKV> simpleScanCond;
	private List<HbaseDataType> orderByList;
	private int limit;
	private List<String> scanqulifier;
	private TreeSet<String> simpleQulifierCond;
	private TreeSet<String> complexQulifierCond;
	private TreeSet<String> qulifierAllCond;
	public PartRowKeyString() {
	}
	
	private String buildByMap(TreeMap<String,String> map){
		 StringBuilder sb=new StringBuilder("");
		 int i=0;
		 for(String q:map.keySet()){
			 sb.append(q).append(Constants.QSPLITTER)
			 .append(map.get(q));
			 if(i++<map.size()-1)
				 sb.append(Constants.REGSPLITER);
		 }
		 return sb.toString();
	}
	
	public Scan buildScan(Scan scan,String familyName,boolean isIdxScan,CasCadeScanMap casMap,String prefix,String subStr,boolean needSetParam){
		if(scan==null){
			scan=new Scan();
			scan.setCaching(20);
			scan.addFamily(familyName.getBytes());
		}
		ComplexRowFilter rf=null;
		if (isIdxScan){
			scan.addColumn(familyName.getBytes(),
					Constants.IDXPREFIX.getBytes());
			FilterList fl = new FilterList();
			buildFliterList(prefix, subStr, fl);
			fl.addFilter(new FirstKeyOnlyFilter());
			scan.setFilter(fl);
			rf = (ComplexRowFilter) fl.getFilters().get(0);
		}
		else {
			if (scanqulifier != null && !scanqulifier.isEmpty()) {
				for (String q : scanqulifier)
					scan.addColumn(familyName.getBytes(), q.getBytes());
			}
			rf = buildComplexRowFilter(prefix,subStr);
			scan.setFilter(rf);
		}
		if(limit>0){
			addPageFilter(scan,limit);
		}
		if (casMap != null)
			rf.setPostScanNext(casMap);
		if (needSetParam)
			setFilterListCond(familyName, rf);
		return scan;
	}

	public void buildFliterList(String prefix, String subStr, FilterList fl) {
		if(fl==null)
			return;
		if(complexScanCond!=null&&complexScanCond.size()>0){
			 ComplexRowFilter rf = buildComplexFilter(prefix);
			 fl.addFilter(rf);
		 }
		 if(simpleScanCond!=null&&simpleScanCond.size()>0){
			 ComplexRowFilter rrf = buildSimpleRowFilter(subStr,null);
			 fl.addFilter(rrf);
		 }
	}
	
	public ComplexRowFilter buildComplexRowFilter(String prefix, String subStr) {
		String expr=null;
		ComplexRowFilter rf=null;
		if(complexScanCond!=null&&complexScanCond.size()>0){
			expr=buildComplexStr(prefix).toString();
		}
		rf = buildSimpleRowFilter(subStr,expr);
		return rf;
	}

	private void setFilterListCond(String familyName,
			ComplexRowFilter rf) {
		 if(!StringUtils.isBlank(familyName))
			rf.setIdxDescfamily(familyName);
		 	rf.setScanRequireCond(this);
	}

	private ComplexRowFilter buildComplexFilter(String prefix) {
		ByteArrayComparable scc=null;
		StringBuilder rexStr = buildComplexStr(prefix);
		scc=new RegexStringComparator(rexStr.toString());
		 ComplexRowFilter rf = new ComplexRowFilter(CompareFilter.CompareOp.EQUAL,scc);
		return rf;
	}

	private StringBuilder buildComplexStr(String prefix) {
		StringBuilder rexStr = new StringBuilder(prefix);
			 int i = 0;
			 for(TreeMap<String,String> m:complexScanCond) {
					rexStr.append(buildByMap(m));
					if (i++ < complexScanCond.size() - 1) {
						rexStr.append("|");
					}
				}
				rexStr.append(").*");
		return rexStr;
	}

	private ComplexRowFilter buildSimpleRowFilter(String subStr,String expr) {
		ListScanOrderedKV listScan=new ListScanOrderedKV();
		if(simpleScanCond!=null&&simpleScanCond.size()>0)
		 listScan.setListScan(simpleScanCond);
		if(orderByList!=null&&orderByList.size()>0)
		 listScan.setOrderBy(orderByList);
		 listScan.setSubStr(subStr);
		 if(!StringUtils.isBlank(expr))
		 listScan.setExpr(expr);
		 ComplexSubstringComparator csc=null;
		try {
			csc = new ComplexSubstringComparator(listScan);
		} catch (IOException e) {
			LOG.error(e.getMessage(),e);
		}
		 ComplexRowFilter rrf = new ComplexRowFilter(CompareFilter.CompareOp.EQUAL,csc);
		return rrf;
	}
	
	
	public MetaScan buildScan(String tbName,String family,MetaTableIndex metaIndex,HTableDescriptor desc,Scan scan,CasCadeScanMap casMap){
		try {
			String rk=desc.getValue(family);
			if(rk==null)
				throw new HbaseScanException("lack of meta data for "+tbName+" "+family);
			RowKeyComposition rkc=JSON.parseObject(rk,RowKeyComposition.class);
			if(rkc==null)
				throw new HbaseScanException("lack of meta data for "+tbName+" "+family);
			//first rank index
			if(rkc.getFamilyColsForRowKey().containsAll(qulifierAllCond)){
				if(orderByList==null||orderByList.isEmpty()||rkc.getOrderBy()==null)
				return new MetaScan(scan,null);
				if(orderByList.size()==1&&orderByList.get(0).equals(rkc.getOrderBy()))
					return new MetaScan(scan,null);	
			}
			MetaIndex mi=null;
			//second rank index
			if((mi=filterIdxScan(tbName,family,qulifierAllCond,metaIndex))!=null){
				return buildIdxScan(casMap, mi,family);
			}
			if(orderByList!=null&&!orderByList.isEmpty()){
				throw new HbaseScanException("lack of idx for the orderBy scan:"+orderByList);
			}
			if(qulifierAllCond.size()<=1)
				throw new HbaseScanException("lack of idx for the this scan");
			MetaScan mms=filterIdxScan(tbName,family,qulifierAllCond,simpleQulifierCond,complexQulifierCond
					,metaIndex,rkc.getFamilyColsForRowKey());
			if(mms!=null){
				MetaScan ms=buildIdxScan(casMap, mms.getMi(),family);
				ms.setComplexQulifierCond(mms.getComplexQulifierCond());
				ms.setSimpleQulifierCond(mms.getSimpleQulifierCond());
				return ms;
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(),e);
			try {
				throw new HbaseScanException("error scan:"+e.getMessage());
			} catch (HbaseScanException e1) {
			}
		}
		return null;
	}

	private void addPageFilter(Scan scan, int limit) {
		Filter f = scan.getFilter();
		FilterList fl = null;
		if (f instanceof FilterList) {
			fl = (FilterList) f;
		} else {
			fl = new FilterList();
			if(f!=null)
			fl.addFilter(f);
		}
		fl.addFilter(new PageFilter(limit+1));
		scan.setFilter(fl);
	}

	private MetaScan buildIdxScan(CasCadeScanMap casMap, MetaIndex mi, String family) {
		StringBuilder rexStr = new StringBuilder(Constants.REGHBASEPREFIIX)
		.append(mi.getIdxName()).append(Constants.SPLITTER).append("(");
		String subStr=new StringBuilder(Constants.REGDELHBASEPREFIIX).append(mi.getIdxName()).toString();
		return new MetaScan(buildScan(null, family,true,casMap,rexStr.toString(),subStr,false),mi);
	}
	
	private MetaIndex filterIdxScan(String tbName,String familyName,TreeSet<String> qulifierAllCond,MetaTableIndex metaIndex) {
		Map<String, Map<String, MetaFamilyIndex>> map = metaIndex
				.getTbIndexMap();
		Map<String, MetaFamilyIndex> mf = map.get(tbName);
		MetaFamilyIndex mlm = mf.get(familyName);
		return MultiIndexFetcher
				.getInstance().genMetaIndex(qulifierAllCond,orderByList,mlm);
		
	}
	
	private MetaScan filterIdxScan(String tbName,String familyName,TreeSet<String> qulifierAllCond,
			TreeSet<String> simpleQulifierCond,TreeSet<String> complexQulifierCond,MetaTableIndex metaIndex,TreeSet<String> rowkSet) {
		Map<String, Map<String, MetaFamilyIndex>> map = metaIndex
				.getTbIndexMap();
		Map<String, MetaFamilyIndex> mf = map.get(tbName);
		MetaFamilyIndex mlm = mf.get(familyName);
		ConcurrentHashMultiset<String> ss=ConcurrentHashMultiset.create(qulifierAllCond);
		List<TreeMap<String,String>> complexQulifierCondcp=new ArrayList<TreeMap<String,String>>();
		List<ScanOrderedKV> simpleQulifierCondcp=new ArrayList<ScanOrderedKV>();
		int complexSize=complexQulifierCond==null?0:complexQulifierCond.size();
		for(String s:ss){
			if(rowkSet!=null&&rowkSet.contains(s)){
				ss.remove(s);
				if(simpleQulifierCond!=null&&simpleQulifierCond.contains(s)){
					simpleQulifierCond.remove(s);
					addSimpleQulifierCondcp(simpleQulifierCondcp,s);
				}
				if(complexQulifierCond!=null&&complexQulifierCond.contains(s)){
					complexQulifierCond.remove(s);
				}
			}
		}
		//complexcond need all in rowk or sencondary key index
		if(complexQulifierCond!=null&&!complexQulifierCond.isEmpty()&&complexQulifierCond.size()<complexSize)
			return null;
		if(complexQulifierCond!=null&&complexQulifierCond.isEmpty()){
			complexQulifierCondcp.addAll(complexScanCond);
			complexScanCond.clear();
		}
		Set<String> set=new HashSet<String>(ss.size());
		set.addAll(ss);
		MetaIndex mi=MultiIndexFetcher.getInstance().genMetaIndex(set,orderByList,mlm);
		if(mi==null)
			return null;
		MetaScan ms=new MetaScan(null,mi);
		ms.setComplexQulifierCond(complexQulifierCondcp);
		ms.setSimpleQulifierCond(simpleQulifierCondcp);
		return ms;
	}
	
	private void addSimpleQulifierCondcp(List<ScanOrderedKV> simpleQulifierCondcp,String s) {
		if(simpleScanCond!=null&&!simpleScanCond.isEmpty())
		for(Iterator<ScanOrderedKV> it=simpleScanCond.iterator();it.hasNext();){
			ScanOrderedKV skv=it.next();
			if(skv.getQulifier().equals(s)){
				simpleQulifierCondcp.add(skv);
				it.remove();
				simpleScanCond.remove(skv);
			}
		}
	}
	
	public ScanOrderStgKV buildScanRange(LinkedHashSet<String> tree){
		for(String x:tree){
			if(simpleQulifierCond!=null&&simpleQulifierCond.contains(x)){
				ScanOrderedKV sok=null;
				for(ScanOrderedKV ss:simpleScanCond){
					if(x.equals(ss.getQulifier())){
						sok=ss;
						ScanOrderedKV skk=build(sok,simpleScanCond);
						if(skk==null){
							if(CompareOp.GREATER.equals(sok.getOp())||CompareOp.GREATER_OR_EQUAL.equals(sok.getOp())){
								return new ScanOrderStgKV(sok,null);
							}
							if(CompareOp.LESS.equals(sok.getOp())||CompareOp.LESS_OR_EQUAL.equals(sok.getOp())){
								return new ScanOrderStgKV(null,sok);
							}
							return new ScanOrderStgKV(sok,sok);
						}
						if(sok.getOp()!=null&&(sok.getOp().equals(CompareOp.GREATER)||sok.getOp().equals(CompareOp.GREATER_OR_EQUAL))){
							if(skk.getOp()!=null&&(skk.getOp().equals(CompareOp.GREATER)||skk.getOp().equals(CompareOp.GREATER_OR_EQUAL)))
								return sok.getValue().compareTo(skk.getValue())>=0?new ScanOrderStgKV(sok,null):new ScanOrderStgKV(skk,null);
							if(skk.getOp()!=null&&(skk.getOp().equals(CompareOp.LESS)||skk.getOp().equals(CompareOp.LESS_OR_EQUAL)))
								return new ScanOrderStgKV(sok,skk);
							if(skk.getOp()!=null&&skk.getOp().equals(CompareOp.EQUAL))
							return sok.getValue().compareTo(skk.getValue())<=0?new ScanOrderStgKV(sok,null):null;
						}
						else if(sok.getOp()!=null&&(sok.getOp().equals(CompareOp.LESS)||sok.getOp().equals(CompareOp.LESS_OR_EQUAL))){
							if(skk.getOp()!=null&&(skk.getOp().equals(CompareOp.LESS)||skk.getOp().equals(CompareOp.LESS_OR_EQUAL)))
								return sok.getValue().compareTo(skk.getValue())<=0?new ScanOrderStgKV(sok,null):new ScanOrderStgKV(skk,null);
							if(skk.getOp()!=null&&(skk.getOp().equals(CompareOp.GREATER)||skk.getOp().equals(CompareOp.GREATER_OR_EQUAL)))
								return new ScanOrderStgKV(skk,sok);
							if(skk.getOp()!=null&&skk.getOp().equals(CompareOp.EQUAL))
							return sok.getValue().compareTo(skk.getValue())>=0?new ScanOrderStgKV(sok,null):null;
						}
						else if(sok.getOp()!=null&&sok.getOp().equals(CompareOp.EQUAL)){
							if(skk.getOp()!=null&&(skk.getOp().equals(CompareOp.LESS)||skk.getOp().equals(CompareOp.LESS_OR_EQUAL)))
								return sok.getValue().compareTo(skk.getValue())<=0?new ScanOrderStgKV(skk,null):null;
							if(skk.getOp()!=null&&(skk.getOp().equals(CompareOp.GREATER)||skk.getOp().equals(CompareOp.GREATER_OR_EQUAL)))
								return sok.getValue().compareTo(skk.getValue())>=0?new ScanOrderStgKV(skk,null):null;
							if(skk.getOp()!=null&&skk.getOp().equals(CompareOp.EQUAL))
								return sok.getValue().compareTo(skk.getValue())==0?new ScanOrderStgKV(skk,null):null;
						}
					}
				}
			}
			if(complexQulifierCond!=null&&complexQulifierCond.contains(x)){
				TreeSet<String> complexValue=new TreeSet<String>();
				for(TreeMap<String,String> treeMap:complexScanCond){
					if(treeMap.containsKey(x)){
						complexValue.add(treeMap.get(x));
					}
				}
				if(complexValue.isEmpty())
					return null;
				else if(complexValue.size()==1){
					ScanOrderedKV ssok=new ScanOrderedKV(x,complexValue.iterator().next(),CompareOp.EQUAL);
					return new ScanOrderStgKV(ssok,ssok);
				}
				else{
					String max=Collections.max(complexValue);
					String min=Collections.min(complexValue);
					ScanOrderStgKV sosktv= new ScanOrderStgKV(new ScanOrderedKV(x,min,CompareOp.GREATER_OR_EQUAL),
							new ScanOrderedKV(x,max,CompareOp.LESS_OR_EQUAL));
					sosktv.setComplexScanOrderList(complexValue);
					return sosktv;
				}
			}
		}
		return null;
	}
	
	private ScanOrderedKV build(ScanOrderedKV sok, List<ScanOrderedKV> simpleScanCond) {
		ScanOrderedKV skk=null;
		for(ScanOrderedKV sk:simpleScanCond){
			if(!sk.equals(sok)&&sk.getQulifier().equals(sok.getQulifier())){
				skk=sk;
				break;
			}
		}
		return skk;
	}

	public PartRowKeyString(List<TreeMap<String,String>> complexScanCond,List<ScanOrderedKV> simpleScanCond,List<String> scanqulifier) {
		if(complexScanCond!=null&&complexScanCond.size()==1){
			if(simpleScanCond==null)
				simpleScanCond=new ArrayList<ScanOrderedKV>();
			TreeMap<String,String> map=complexScanCond.get(0);
			for(String q:map.keySet()){
				ScanOrderedKV sok=new ScanOrderedKV(q,map.get(q),CompareOp.EQUAL);
				simpleScanCond.add(sok);
			}
			this.simpleScanCond=simpleScanCond;
		}
		else 
		{
			this.complexScanCond=complexScanCond;
			this.simpleScanCond=simpleScanCond;
		}
		this.scanqulifier=scanqulifier;
		check();
	}

	private void check(){
		if(complexScanCond==null&&simpleScanCond==null)
			throw new IllegalStateException("complexScanCond and simpleScanCond can not be both null"); 
		qulifierAllCond=new TreeSet<String>();
		if(complexScanCond!=null&&!complexScanCond.isEmpty()){
			complexQulifierCond=new TreeSet<String>();
			Map<String,String> map=complexScanCond.get(0);
			for(String s:map.keySet()){
				qulifierAllCond.add(s);
				complexQulifierCond.add(s);
			}
			for(Map<String,String> m:complexScanCond){
				if(m.size()!=map.size())
					throw new IllegalStateException("all map in complexScanCondList need be the same size"); 
				for(String s:m.keySet()){
					if(!map.containsKey(s))
					   throw new IllegalStateException("all map in complexScanCondList need own the same key"); 
				}
			}
		}
		if(simpleScanCond!=null){
			simpleQulifierCond=new TreeSet<String>();
			for(ScanOrderedKV so:simpleScanCond){
				if(so.getQulifier()==null||so.getValue()==null)
					 throw new IllegalStateException("all ScanOrderedKV in simpleScanCondList must not be null"); 
				qulifierAllCond.add(so.getQulifier());
				simpleQulifierCond.add(so.getQulifier());
			}
		}
		if(scanqulifier!=null&&scanqulifier.isEmpty())
			throw new IllegalStateException("scanqulifier must not be empty List"); 
	}

	public List<TreeMap<String, String>> getComplexScanCond() {
		return complexScanCond;
	}

	public void setComplexScanCond(List<TreeMap<String, String>> complexScanCond) {
		this.complexScanCond = complexScanCond;
	}

	public List<ScanOrderedKV> getSimpleScanCond() {
		return simpleScanCond;
	}

	public void setSimpleScanCond(List<ScanOrderedKV> simpleScanCond) {
		this.simpleScanCond = simpleScanCond;
	}

	public List<String> getScanqulifier() {
		return scanqulifier;
	}

	public void setScanqulifier(List<String> scanqulifier) {
		this.scanqulifier = scanqulifier;
	}

	public TreeSet<String> getSimpleQulifierCond() {
		return simpleQulifierCond;
	}

	public void setSimpleQulifierCond(TreeSet<String> simpleQulifierCond) {
		this.simpleQulifierCond = simpleQulifierCond;
	}

	public TreeSet<String> getComplexQulifierCond() {
		return complexQulifierCond;
	}

	public void setComplexQulifierCond(TreeSet<String> complexQulifierCond) {
		this.complexQulifierCond = complexQulifierCond;
	}

	public TreeSet<String> getQulifierAllCond() {
		return qulifierAllCond;
	}

	public void setQulifierAllCond(TreeSet<String> qulifierAllCond) {
		this.qulifierAllCond = qulifierAllCond;
	}

	public List<HbaseDataType> getOrderByList() {
		return orderByList;
	}

	public void setOrderByList(List<HbaseDataType> orderByList) {
		this.orderByList = orderByList;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}
}