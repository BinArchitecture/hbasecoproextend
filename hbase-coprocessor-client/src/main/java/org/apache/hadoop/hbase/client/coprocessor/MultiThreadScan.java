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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.agg.AggHandlerProxy;
import org.apache.hadoop.hbase.client.coprocessor.agg.AggHandlerProxy.AggEnum;
import org.apache.hadoop.hbase.client.coprocessor.agg.AggResultHandlerProxy;
import org.apache.hadoop.hbase.client.coprocessor.agg.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.AggGroupByHandlerProxy;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.AggGroupByHandlerProxy.AggGroupByEnum;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.AggGroupByResultHandlerProxy;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.AggregationGroupByClient;
import org.apache.hadoop.hbase.client.coprocessor.model.AggGroupResult;
import org.apache.hadoop.hbase.client.coprocessor.model.AggResult;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType;
import org.apache.hadoop.hbase.client.coprocessor.model.ScanCond;
import org.apache.hadoop.hbase.client.coprocessor.model.ScanResult;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.ScanOrderStgKV;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CasCadeScanMap;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.PagerList;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lppz.elasticsearch.LppzEsComponent;
import com.lppz.elasticsearch.PrepareBulk;
import com.lppz.elasticsearch.result.SearchAllResult;
import com.lppz.elasticsearch.result.SearchResult;
import com.lppz.elasticsearch.search.SearchCondition;
import com.lppz.elasticsearch.search.SortBy;
@SuppressWarnings("deprecation") 
public class MultiThreadScan {
	private static final Logger logger = LoggerFactory.getLogger(MultiThreadScan.class);
	private ScanCond sc;
	ExecutorService httpExecutor = Executors.newCachedThreadPool();
	public MultiThreadScan(){}
	public MultiThreadScan(ScanCond sc){
		this.sc=sc;
	}
	
	public List<ScanResult> multiScan(String tableName,RowKeyComposition rkc,
			HTablePool hTablePool, boolean... isNature) throws Exception{
		HTableInterface table =hTablePool.getTable(tableName);
		if(sc.getStartRow()!=null||sc.getEndRow()!=null)
			return HbaseUtil.scanNormal(tableName, sc.getCasCadeScanMap(), table,sc.buildScan(tableName));
		ScanOrderStgKV sstk=sc.getPr().buildScanRange(rkc.getFamilyColsNeedIdx()); 
		if(sstk==null){
			return HbaseUtil.scanNormal(tableName, sc.getCasCadeScanMap(), table,sc.buildScan(tableName));
		}
		try {
//			List<ScanResult> listScr=idxHbaseClient.scanIdxRangeTb(TableName.valueOf(Constants.IDXTABLENAMEPREFIX+tableName), sstk, sc.getFamiliName(),sc);
//			return listScr;
			List<String> prefixList=new ArrayList<String>();
			if(isNature==null||isNature.length==0){
				List<ScanOrderStgKV> listSstk=sstk.buildComplexList();
				for(ScanOrderStgKV sstgkv:listSstk){
					List<String> tmpprefixList=buildPrefixListByEs(tableName,sstgkv,sc.getFamiliName(),sc.isReversed());//idxHbaseClient.scanIdxRangeTb(TableName.valueOf(Constants.IDXTABLENAMEPREFIX+tableName), sstgkv, sc.getFamiliName(),sc.isReversed());
					if(!CollectionUtils.isEmpty(tmpprefixList))
						prefixList.addAll(tmpprefixList);
				}
			}
			else{
				sstk=sc.getOrgPr().buildScanRange(rkc.getFamilyColsNeedIdx()); 
				List<ScanOrderStgKV> listSstk=sstk.buildComplexList();
				for(ScanOrderStgKV sstgkv:listSstk){
					List<String> tmpprefixList=new ArrayList<String>(1);
					if(sstgkv.isMainId(rkc)){
						tmpprefixList.add(sstgkv.buildPrefixByMainId());
					}
					else
						tmpprefixList=buildPrefixListByEs(tableName,sstgkv,sc.getFamiliName(),sc.isReversed());
					if(!CollectionUtils.isEmpty(tmpprefixList))
						prefixList.addAll(tmpprefixList);
				}
			}
			long tEnd = System.currentTimeMillis(); 
			System.out.println("tEnd:"+tEnd);
			List<ScanResult> listScr=multiScan(tableName, hTablePool, prefixList,false);
			return listScr;
		} catch (Throwable e) {
			logger.error(e.getMessage(),e);
		}
		return null;
	}
	
	public PagerList multiScanByPage(String tableName,LinkedHashSet<String> lhs,
			HTablePool hTablePool, Boolean isOrderBy,boolean... isNature) throws Exception{
//		HTableInterface table =hTablePool.getTable(tableName);
		ScanOrderStgKV sstk=sc.getPr().buildScanRange(lhs);
		if(sstk==null){
			return null;
//			return HbaseUtil.scanNormal(tableName, sc.getCasCadeScanMap(), table,sc.buildScan(tableName));
		}
		try {
			List<ScanOrderStgKV> listSstk=sstk.buildComplexList();
			List<String> prefixList=new ArrayList<String>();
			for(ScanOrderStgKV sstgkv:listSstk){
				int limit=sc.getPr().getLimit();
				int startRow=Integer.parseInt(sc.getStartRow());
				List<String> tmpprefixList=buildPrefixListByEsScroll(tableName,sstgkv,sc.getFamiliName(),false,startRow,limit,isOrderBy);//idxHbaseClient.scanIdxRangeTb(TableName.valueOf(Constants.IDXTABLENAMEPREFIX+tableName), sstgkv, sc.getFamiliName(),sc.isReversed());
				if(!CollectionUtils.isEmpty(tmpprefixList))
					prefixList.addAll(tmpprefixList);
			}
			long tEnd = System.currentTimeMillis(); 
			System.out.println("tEnd:"+tEnd);
			List<ScanResult> listScr=multiScan(tableName, hTablePool, prefixList,true);
			PagerList pg=new PagerList();
			pg.setListResult(listScr);
			pg.setPrefixList(prefixList);
			pg.sort();
			return pg;
		} catch (Throwable e) {
			logger.error(e.getMessage(),e);
		}
		return null;
	}
	
	private List<String> buildPrefixListByEs(String tbName,
			ScanOrderStgKV sstgkv, String familiName, boolean reversed) {
		SearchCondition searchCond=sstgkv.buildSearchCond(tbName,familiName,reversed);
		searchCond.setSearchType(SearchType.QUERY_AND_FETCH);
		SearchAllResult sar=LppzEsComponent.getInstance().search(searchCond);
		List<String> listPrefix=new ArrayList<String>(sar.getResultSearchList().size());
		for(SearchResult sr:sar.getResultSearchList()){
			EsIdxHbaseType type=(EsIdxHbaseType)sr.getSource();
			listPrefix.add(type.getPrefix());
		}
		return listPrefix;
	}
	
	private List<String> buildPrefixListByEsScroll(String tbName,
			ScanOrderStgKV sstgkv, String familiName, boolean reversed,int startRow,int limit, Boolean isOrderBy) {
		SearchCondition searchCond=sstgkv.buildSearchCond(tbName,familiName,reversed);
		if(isOrderBy!=null){
			List<SortBy> sortList=new ArrayList<SortBy>();
			SortBy sob=new SortBy("idxValue",isOrderBy?SortOrder.ASC:SortOrder.DESC);
			sortList.add(sob);
			searchCond.setSortList(sortList);
		}
		final List<String> listPrefix=new ArrayList<String>(limit);
		PrepareBulk prepareBulk=new PrepareBulk(){

			@Override
			public void bulk(List<SearchResult> listRes) {
				for(SearchResult sr:listRes){
					EsIdxHbaseType type=(EsIdxHbaseType)sr.getSource();
					listPrefix.add(type.getPrefix());
					logger.info(type.getPrefix()+" has been added:"+listPrefix.size());
				}
			}
		};
		LppzEsComponent.getInstance().scrollSearchPager(new String[]{searchCond.getIdxName()}, searchCond.getTypes(), searchCond.getSearchQuery(),startRow, limit,searchCond.getSortList(),60000,prepareBulk);
		return listPrefix;
	}
	
	public AggGroupResult multiGroupScan(String tableName,LinkedHashSet<String> lhs,AggregationGroupByClient aggregationGroupByClient,
			HTablePool hTablePool,AggGroupByEnum type,List<String> groupby,String classType) throws Exception{
		Scan scan=sc.buildScan(tableName);
		String qualiFier=sc.getPr().getScanqulifier()==null?null:sc.getPr().getScanqulifier().get(0);
		if(sc.getStartRow()!=null||sc.getEndRow()!=null)
			return AggGroupByHandlerProxy.getInstance().groupBy(tableName, qualiFier,classType, type, groupby, scan, aggregationGroupByClient, null);
		ScanOrderStgKV sstk=sc.getPr().buildScanRange(lhs);
		if(sstk==null){
			return AggGroupByHandlerProxy.getInstance().groupBy(tableName, qualiFier,classType, type, groupby, scan, aggregationGroupByClient, null);
		}
		try {
			List<String> prefixList=buildPrefixListByEs(tableName,sstk, sc.getFamiliName(),sc.isReversed());//idxHbaseClient.scanIdxRangeTb(TableName.valueOf(Constants.IDXTABLENAMEPREFIX+tableName), sstk, sc.getFamiliName(),sc.isReversed());
			AtomicInteger ai=new AtomicInteger(0);
			List<Set<String>> rangeAlllist=new ArrayList<Set<String>>(build(prefixList).values());
			List<GroupByScanRunable> lls=new ArrayList<GroupByScanRunable>(rangeAlllist.size());
			for(Set<String> rangeList:rangeAlllist){
				Scan s=buildBigRangeScan(scan,rangeList);
				GroupByScanRunable gsrn=new GroupByScanRunable(type,aggregationGroupByClient,s,tableName, qualiFier, hTablePool, ai,classType,groupby, rangeList);
				lls.add(gsrn);
				httpExecutor.execute(gsrn);
			}
			while(true){
				if(ai.get()>=rangeAlllist.size()){
					AggGroupResult aggResult=new AggGroupResult();
					AggGroupByResultHandlerProxy.getInstance().aggResult(lls, type, aggResult, classType);
					return aggResult;
				}
				Thread.sleep(100);
			}
		} catch (Throwable e) {
			e.printStackTrace();
			logger.error(e.getMessage(),e);
		}
		return null;
	}
	
	public AggResult multiAggScan(String tableName, LinkedHashSet<String> lhs,
			org.apache.hadoop.hbase.client.coprocessor.agg.AggregationClient aggregationClient, HTablePool hTablePool,AggEnum type, String classType) throws Exception {
		Scan scan=sc.buildScan(tableName);
		String qualiFier=sc.getPr().getScanqulifier()==null?null:sc.getPr().getScanqulifier().get(0);
		if(sc.getStartRow()!=null||sc.getEndRow()!=null)
			return AggHandlerProxy.getInstance().agg(tableName, qualiFier, classType, type, scan, aggregationClient, null);
		ScanOrderStgKV sstk=sc.getPr().buildScanRange(lhs);
		if(sstk==null){
			return AggHandlerProxy.getInstance().agg(tableName, qualiFier,classType,type, scan, aggregationClient, null);
		}
		try {
			List<String> prefixList=buildPrefixListByEs(tableName,sstk, sc.getFamiliName(),sc.isReversed());//idxHbaseClient.scanIdxRangeTb(TableName.valueOf(Constants.IDXTABLENAMEPREFIX+tableName), sstk, sc.getFamiliName(),sc.isReversed());
			AtomicInteger ai=new AtomicInteger(0);
			List<Set<String>> rangeAlllist=new ArrayList<Set<String>>(build(prefixList).values());
			List<ScanAggRunable> lls=new ArrayList<ScanAggRunable>(rangeAlllist.size());
			for(Set<String> rangeList:rangeAlllist){
				Scan s=buildBigRangeScan(scan,rangeList);
				ScanAggRunable gsrn=new ScanAggRunable(tableName, aggregationClient, hTablePool, type, classType, qualiFier, s, ai, rangeList);
				lls.add(gsrn);
				httpExecutor.execute(gsrn);
			}
			while(true){
				if(ai.get()>=rangeAlllist.size()){
					AggResult result=new AggResult();
					AggResultHandlerProxy.getInstance().aggResult(lls, type, result,classType);
					return result;
				}
				Thread.sleep(100);
			}
		} catch (Throwable e) {
			e.printStackTrace();
			logger.error(e.getMessage(),e);
		}
		
		return null;
	}
	
	
	private Scan buildBigRangeScan(Scan scan, Set<String> rangeList) {
		Scan s=HbaseUtil.cloneScan(scan);
		s.setStartRow(new StringBuilder(Collections.min(rangeList)).append(Constants.REGTABLEHBASESTART).toString().getBytes());
		s.setStopRow(new StringBuilder(Collections.max(rangeList)).append(Constants.REGTABLEHBASESTOP).toString().getBytes());
		return s;
	}
	
	public List<ScanResult> multiScan(String tableName,
			HTablePool hTablePool, List<String> tree,boolean isPager)
					throws InterruptedException {
		List<List<Scan>> listScan=buildMultiScan(tableName,buildSeqMap(build(tree),isPager));
		List<ScanRunable> lls=new ArrayList<ScanRunable>(listScan.size());
		AtomicInteger ai=new AtomicInteger(0);
		for(final List<Scan> list:listScan){
			ScanRunable srn=new ScanRunable(list, tableName, sc.getCasCadeScanMap(), hTablePool,ai);
			lls.add(srn);
			httpExecutor.execute(srn);
		}
		while(true){
			if(ai.get()>=listScan.size()){
				List<ScanResult> lsr=new ArrayList<ScanResult>();
				for(ScanRunable srn:lls){
					lsr.addAll(srn.getListResult());
				}
				return lsr;
			}
			Thread.sleep(100);
		}
	}

	private Map<String,Set<List<String>>> buildSeqMap(Map<String,Set<String>> rangeMap,boolean isPager) {
		Map<String,Set<List<String>>> map=new HashMap<String,Set<List<String>>>();
		for(String s:rangeMap.keySet()){
			Set<String> set=rangeMap.get(s);
			Set<List<String>> ss=HbaseUtil.buildSet(set,isPager);
			map.put(s, ss);
		}
		return map;
	}
	
	private Map<String,Set<String>> build(List<String> tree) {
		Map<String,Set<String>> map=new HashMap<String,Set<String>>();
		for(String s:tree){
			String prefix=s.substring(0,1);
			Set<String> l=map.get(prefix);
			if(l==null)
				l=new TreeSet<String>();
			l.add(s);
			map.put(prefix, l);
		}
		return map;
	}

	private List<List<Scan>> buildMultiScan(String tableName,Map<String, Set<List<String>>> map) {
		List<List<Scan>> listScan=new ArrayList<List<Scan>>();
		Scan orScan=sc.buildScan(tableName);
		for(Set<List<String>> r:map.values()){
			List<Scan> ls=new ArrayList<Scan>();
			for(List<String> s:r){
				Scan scan=HbaseUtil.cloneScan(orScan);
				scan.setStartRow(((scan.isReversed()?Collections.max(s)+Constants.REGTABLEHBASESTOP:Collections.min(s)+Constants.REGTABLEHBASESTART)).getBytes());
				scan.setStopRow(((scan.isReversed()?Collections.min(s)+Constants.REGTABLEHBASESTART:Collections.max(s)+Constants.REGTABLEHBASESTOP)).getBytes());
				ls.add(scan);
			}
			listScan.add(ls);
		}
		return listScan;
	}
	
	private class ScanRunable implements Runnable{
		AtomicInteger ai;
		private List<Scan> listScan;
		private String tableName;
		private CasCadeScanMap casCadeScanMap;
		private HTablePool hTablePool;
		private List<ScanResult> listResult=Collections.synchronizedList(new ArrayList<ScanResult>());
		public ScanRunable(List<Scan> listScan,String tableName,CasCadeScanMap casCadeScanMap,HTablePool hTablePool, AtomicInteger ai){
			this.listScan=listScan;
			this.tableName=tableName;
			this.casCadeScanMap=casCadeScanMap;
			this.hTablePool=hTablePool;
			this.ai=ai;
		}
		
		@Override
		public void run() {
			if(listScan==null) return;
			final HTableInterface hti=hTablePool.getTable(tableName);
			 ExecutorService pool= Executors.newFixedThreadPool(20);
			final AtomicInteger a=new AtomicInteger(0);
			for(final Scan scan:listScan){
				pool.execute(new Runnable() {
					@Override
					public void run() {
						try {
							listResult.addAll(HbaseUtil.scanNormal(tableName, casCadeScanMap,hti, scan));
						} catch (Exception e) {
							logger.error(e.getMessage(),e);
						}
						finally{
							a.getAndAdd(1);
						}
					}
				});
			}
			while(true){
				if(a.get()==listScan.size()){
					pool.shutdown();
					break;
				}
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
			}
			if(ai!=null)
				ai.getAndAdd(1);
			try {
				hti.close();
			} catch (IOException e) {
			}
		}
		public List<ScanResult> getListResult() {
			return listResult;
		}
	}
	
	public class GroupByScanRunable implements Runnable{
		AtomicInteger ai;
		private Set<String> rangeList;
		private String tableName;
		private AggGroupByEnum type;
		private HTablePool hTablePool;
		private Scan scan;
		private String qualifier;
		private String classType;
		private List<String> groupBy;
		AggregationGroupByClient aggregationGroupByClient;
		private AggGroupResult result;
		public GroupByScanRunable(AggGroupByEnum type,AggregationGroupByClient aggregationGroupByClient,Scan scan,String tableName,String qualifier,HTablePool hTablePool, AtomicInteger ai,String classType,
				List<String> groupBy,Set<String> rangeList){
			this.scan=scan;
			this.tableName=tableName;
			this.classType=classType;
			this.qualifier=qualifier;
			this.hTablePool=hTablePool;
			this.ai=ai;
			this.groupBy=groupBy;
			this.type=type;
			this.rangeList=rangeList;
			this.aggregationGroupByClient=aggregationGroupByClient;
		}
		@Override
		public void run() {
			if(scan==null) return;
			HTableInterface hti=hTablePool.getTable(tableName);
			this.result=AggGroupByHandlerProxy.getInstance().groupBy(tableName, qualifier,classType,type, groupBy, scan, aggregationGroupByClient, rangeList);
			if(ai!=null)
			ai.getAndAdd(1);
			try {
				hti.close();
			} catch (IOException e) {
			}
		}
		public AggGroupResult getResult() {
			return result;
		}
	}
	
	
	public class ScanAggRunable implements Runnable{
		private String tableName;
		private org.apache.hadoop.hbase.client.coprocessor.agg.AggregationClient aggregationClient;
		private HTablePool hTablePool;
		private AggEnum type;
		private String classType;
		private AggResult result;
		private String qualifier;
		private Scan scan;
		AtomicInteger ai;
		private Set<String> rangeList;
		
		
		public ScanAggRunable(String tableName, AggregationClient aggregationClient,
				HTablePool hTablePool, AggEnum type, String classType,
				String qualifier, Scan scan, AtomicInteger ai,Set<String> rangeList) {
			this.tableName = tableName;
			this.aggregationClient = aggregationClient;
			this.hTablePool = hTablePool;
			this.type = type;
			this.classType = classType;
			this.qualifier = qualifier;
			this.scan = scan;
			this.ai = ai;
			this.rangeList = rangeList;
		}

		@Override
		public void run() {
			if(scan==null) return;
			HTableInterface hti=hTablePool.getTable(tableName);
			this.result= AggHandlerProxy.getInstance().agg(tableName, qualifier, classType, type, scan, aggregationClient, rangeList);
			try {
				hti.close();
			} catch (IOException e) {
			}
			finally{
				if(ai!=null)
					ai.getAndAdd(1);
			}
		}
		
		public AggResult getResult() {
			return result;
		}
		
	}
}
