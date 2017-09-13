/**
a * Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.hadoop.hbase.coprocessor.observer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbaseDeleteException;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbaseIdxLockedException;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbaseIdxRegioninitException;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbaseIdxScanForOrderException;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbasePutException;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbaseScanException;
import org.apache.hadoop.hbase.client.coprocessor.exception.kafka.HbaseKafkaInitException;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType.Operation;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.IdxKafkaMsg;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.ListScanOrderedKV;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaFamilyIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaScan;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaTableIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CasCadeScan;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.PartRowKeyString;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.ScanOrderedKV;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.StringList;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.client.coprocessor.util.ReFelctionUtil;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.kafka.consumer.IdxHbaseLockedConsumer;
import org.apache.hadoop.hbase.coprocessor.kafka.consumer.IdxHbaseSyncMetaFlushConsumer;
import org.apache.hadoop.hbase.coprocessor.kafka.consumer.IdxHbaseSyncMetaMapConsumer;
import org.apache.hadoop.hbase.coprocessor.kafka.consumer.listener.IdxHbaseLockedConsumerListener;
import org.apache.hadoop.hbase.coprocessor.kafka.consumer.listener.IdxHbaseSyncMetaFlushConsumerListener;
import org.apache.hadoop.hbase.coprocessor.kafka.consumer.listener.IdxHbaseSyncMetaMapConsumerListener;
import org.apache.hadoop.hbase.coprocessor.kafka.producer.IdxHBaseKafkaEsRegionServerProducer;
import org.apache.hadoop.hbase.coprocessor.observer.handler.IndexRegionObserverPutHandler;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.ComplexRowFilter;
import org.apache.hadoop.hbase.filter.ComplexSubstringComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterWrapper;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.KeeperException;

import com.alibaba.fastjson.JSON;
import com.lppz.util.kafka.consumer.listener.KafkaConsumerListener;
import com.lppz.util.kryo.KryoUtil;

@SuppressWarnings("deprecation")
public class IndexRegionObserver extends HBaseRegionObserver {
	public static IdxHBaseKafkaEsRegionServerProducer getProducer() {
		return producer;
	}

	public static HTablePool getPool() {
		return pool;
	}

	private static final Log LOG = LogFactory.getLog(IndexRegionObserver.class
			.getName());
	private static HTablePool pool;
	private static IdxHBaseKafkaEsRegionServerProducer producer;
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,
			Put put, WALEdit edit, Durability durability) throws IOException {
		if (checkprePut(put))
			return;
		HTableDescriptor hd = e.getEnvironment().getRegion().getTableDesc();
		if (metaIndex == null
				|| metaIndex.getTbIndexNameMap().get(hd.getNameAsString()) == null)
			return;
		String familyName = getFamilyNameByDel(put.getFamilyCellMap().keySet());
		if (checkIsIdxLocked(lockedIdxMap, put, hd, familyName)){
			throw new HbaseIdxLockedException(hd.getNameAsString()+":"+familyName+" is blocked insert by idx lock");
		}
		HbaseUtil.checkprePutValid(put);
		handlePutIdxList(put, hd.getNameAsString(), e.getEnvironment()
				.getRegion(), familyName);
	}

	@SuppressWarnings("rawtypes")
	private void handlePutIdxList(Put put, String tbName, Region region,
			String familyName) throws HbasePutException {
		if (metaIndex == null)
			return;
		Map<String, Map<String, MetaFamilyIndex>> map = metaIndex
				.getTbIndexMap();
		Map<String, MetaFamilyIndex> mf = map.get(tbName);
		MetaFamilyIndex mlm = mf.get(familyName);
		if (mlm == null)
			return;
		Map<StringList, MetaIndex> multiIndexMap = mlm.getMultiIndexMap();
		List<Cell> l = put.getFamilyCellMap().values().iterator().next();
		HTableDescriptor desc = region.getTableDesc();
		RowKeyComposition rkc = JSON.parseObject(desc.getValue(familyName),
				RowKeyComposition.class);
		Map<String, Cell> mapk = HbaseUtil.buildMultiCell(l);
		if(HbaseUtil.checkRkcIsUpdateMainCol(put, rkc, mapk))
			throw new HbasePutException("can not update hbase mainKey:"+rkc.getMainHbaseCfPk().getOidQulifierName()+" in put "+Bytes.toString(put.getRow()));
//		HTableInterface hti = pool.getTable(Constants.IDXTABLENAMEPREFIX
//				+ tbName);
		Map<String, List> mapList = null;
		if (rkc.getFamilyColsNeedIdx() != null) {
			mapList = IndexRegionObserverPutHandler.getInstance().handleIdxRow(tbName,
					put, region, mapk, familyName, rkc);
			try {
				put.setAttribute(Constants.HBASEUPDATEROWFORUPDATE, KryoUtil.kyroSeriLize(mapList, -1));
			} catch (IOException e) {
				LOG.error(e.getMessage(),e);
			}
//			handleHbaseTbListPut(mapList);
		}
		IndexRegionObserverPutHandler.getInstance().handleRegionRow(put,
				tbName, mapk, familyName, region, multiIndexMap);
//		handleHbaseListIdxTbDel(mapList, hti, region, familyName);
	}

//	@SuppressWarnings({ "unchecked", "rawtypes" })
//	private void handleHbaseListIdxTbDel(Map<String, List> mapList,
//			HTableInterface hti, Region region, String familyName) {
//		if (mapList != null) {
//			List<Delete> listDel = mapList.get(Constants.HBASEUPDATEROWFORDEL);
//			if (listDel != null && !listDel.isEmpty()) {
//				try {
//					op: for (Iterator<Delete> it = listDel.iterator(); it
//							.hasNext();) {
//						Delete del = it.next();
//						Scan scan = buildDelScan(region, del, familyName);
//						RegionScanner isxRs = region.getScanner(scan);
//						boolean hasMoreRows = false;
//						do {
//							List<Cell> cells = new ArrayList<Cell>();
//							hasMoreRows = isxRs.next(cells);
//							if (!cells.isEmpty()) {
//								it.remove();
//								listDel.remove(del);
//								continue op;
//							}
//						} while (hasMoreRows);
//					}
//					if (listDel != null && !listDel.isEmpty())
//						hti.delete(listDel);
//				} catch (Exception e) {
//					LOG.error(e.getMessage(), e);
//					try {
//						throw new HbaseDeleteException(e.getMessage());
//					} catch (HbaseDeleteException e1) {
//					}
//				}
//			}
//		}
//		if (hti != null) {
//			try {
//				hti.close();
//			} catch (IOException e) {
//			}
//		}
//	}

//	private Scan buildDelScan(Region region, Delete del, String familyName) {
//		String[] rowArray = Bytes.toString(del.getRow()).split(
//				Constants.SPLITTER);
//		Scan scan = new Scan();
//		scan.setStartRow(new StringBuilder(rowArray[2])
//				.append(Constants.REGTABLEHBASESTART).toString().getBytes());
//		scan.setStopRow(new StringBuilder(rowArray[2])
//				.append(Constants.REGTABLEHBASESTOP).toString().getBytes());
//		FilterList fl = new FilterList();
//		fl.addFilter(new RowFilter(CompareOp.EQUAL, new SubstringComparator(
//				rowArray[0].replaceFirst(Constants.QLIFIERSPLITTER,
//						Constants.QSPLITTER))));
//		fl.addFilter(new FirstKeyOnlyFilter());
//		scan.setFilter(fl);
//		scan.addFamily(familyName.getBytes());
//		return scan;
//	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void handleHbaseTbListPut(Map<String, List> mapList) {
		List<EsIdxHbaseType> listPut = mapList.get(Constants.HBASEUPDATEROWFORUPDATE);
		if (listPut != null && !listPut.isEmpty()) {
			try {
				for(EsIdxHbaseType t:listPut)
				producer.sendMsg(t, t.getRowKey());
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
				try {
					throw new HbasePutException(e.getMessage());
				} catch (HbasePutException e1) {
				}
			}
		}
	}

	private Result fetchpreUpdatePut(byte[] row, Region region, String family,
			List<String> cols) {
		Get get = new Get(row);
		if (family != null) {
			get.addFamily(family.getBytes());
			if (cols != null)
				for (String col : cols)
					get.addColumn(family.getBytes(), col.getBytes());
		}
		Result r = null;
		try {
			r = region.get(get);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
			try {
				throw new HbasePutException(e.getMessage());
			} catch (HbasePutException e1) {
			}
		}
		return r;
	}

	private static volatile Map<String, Map<String, Boolean>> lockedIdxMap;

	private static MetaTableIndex metaIndex;

	public static MetaTableIndex getMetaIndex() {
		return metaIndex;
	}

	private RecoverableZooKeeper rz;

	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		if (e instanceof RegionCoprocessorEnvironment) {
			RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment) e;
			try {
				if (metaIndex == null) {
					synchronized (rce) {
						if (metaIndex == null) {
							rz = rce.getRegionServerServices().getZooKeeper()
									.getRecoverableZooKeeper();
							metaIndex = HbaseUtil.initMetaIndex(rz);
							pool = new HTablePool(e.getConfiguration(), 5000);
							initLockedIdxMap();
							producer=new IdxHBaseKafkaEsRegionServerProducer(e.getConfiguration());
							initKafkaConsumer(e.getConfiguration());
						}
					}
				}
			} catch (KeeperException | InterruptedException e1) {
				LOG.error(e1.getMessage(), e1);
				throw new HbaseIdxRegioninitException(e1.getMessage());
			}
		}
	}

	private void initLockedIdxMap() {
		lockedIdxMap = new HashMap<String, Map<String, Boolean>>();
		for (String tbName : metaIndex.getTbIndexNameMap().keySet()) {
			Map<String, Boolean> mm = new HashMap<String, Boolean>();
			for (String familyName : metaIndex.getTbIndexNameMap().get(tbName)
					.keySet()) {
				mm.put(familyName, false);
			}
			lockedIdxMap.put(tbName, mm);
		}
	}

	private void initKafkaConsumer(Configuration cf) {
		IdxHbaseSyncMetaMapConsumer ihsc = new IdxHbaseSyncMetaMapConsumer();
		KafkaConsumerListener<IdxKafkaMsg> kafkaListener = new IdxHbaseSyncMetaMapConsumerListener(
				metaIndex);
		ihsc.setKafkaListener(kafkaListener);
		IdxHbaseSyncMetaFlushConsumer ihfc = new IdxHbaseSyncMetaFlushConsumer();
		KafkaConsumerListener<MetaTableIndex> mListener = new IdxHbaseSyncMetaFlushConsumerListener(
				metaIndex);
		ihfc.setKafkaListener(mListener);
		IdxHbaseLockedConsumer ihlc = new IdxHbaseLockedConsumer();
		KafkaConsumerListener<IdxKafkaMsg> iListener = new IdxHbaseLockedConsumerListener(
				lockedIdxMap);
		ihlc.setKafkaListener(iListener);
		try {
			ihsc.doInit(cf);
			ihfc.doInit(cf);
			ihlc.doInit(cf);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			try {
				throw new HbaseKafkaInitException(e.getMessage());
			} catch (HbaseKafkaInitException e1) {
			}
		}
	}

	@Override
	public RegionScanner postScannerOpen(
			ObserverContext<RegionCoprocessorEnvironment> e, Scan scan,
			RegionScanner s) throws IOException {
		HTableDescriptor hd = e.getEnvironment().getRegion().getTableDesc();
		if (metaIndex == null
				|| metaIndex.getTbIndexNameMap().get(hd.getNameAsString()) == null)
			return s;
		ComplexRowFilter rf = getSubIdxRowFilter(scan);
		if (rf == null)
			return s;
		PartRowKeyString prks = rf.getScanRequireCond();
		MetaScan idxscan = prks.buildScan(hd.getNameAsString(),
				rf.getIdxDescfamily(), metaIndex, hd, scan,
				rf.getPostScanNext());
		if(idxscan == null)
			throw new HbaseScanException("lack of idx for the this scan");
		if (idxscan.getMi() == null)
			return s;
		boolean mark=setRangeidxScan(scan, idxscan,rf.getIdxDescfamily(),!CollectionUtils.isEmpty(prks.getOrderByList()),e.getEnvironment().getRegion());
		byte[] byteId = scan.getAttribute(Constants.HBASEMAINKEY);
		String cascadeId = byteId==null?null:Bytes.toString(byteId);
		addIdxCascadeId(idxscan, byteId, cascadeId);
		idxscan.getScan().setReversed(scan.isReversed());
		RegionScanner isxRs = e.getEnvironment().getRegion()
				.getScanner(idxscan.getScan());
		boolean hasMoreRows = false;
		List<byte[]> rowll = new ArrayList<byte[]>();
		do {
			List<Cell> cells = new ArrayList<Cell>();
			long tStart = System.currentTimeMillis();
			hasMoreRows = isxRs.next(cells);
			long tEnd = System.currentTimeMillis();
			if (!cells.isEmpty()) {
				LOG.debug(tEnd - tStart + "millions");
				rowll.add(cells.get(0).getRow());
			}
		} while (hasMoreRows);
		if (rowll.isEmpty())
			return s;
		Scan scan1 = buildIndexFilter(rf, rowll, e.getEnvironment().getRegion()
				.getTableDesc().getNameAsString(), idxscan,
				prks.getScanqulifier(),!CollectionUtils.isEmpty(prks.getOrderByList()));
		if(mark)
			scan1.setStartRow(scan.getStartRow());
		else{
			String[] ss=Bytes.toString(scan.getStartRow()).split(Constants.SPLITTER);
			scan1.setStartRow((ss[0]+Constants.REGTABLEHBASESTART).getBytes());
		}
		if(scan.isReversed())
			scan1.setStopRow(Bytes.toString(scan.getStopRow()).replaceFirst(Constants.REGTABLEHBASESTART, Constants.REGTABLEHBASESTOP).getBytes());
		else
			scan1.setStopRow(scan.getStopRow());
		removeidxScanCascadeId(idxscan, cascadeId);
		byte[] parentId = scan.getAttribute(Constants.HBASEPARENTKEY);
		if(parentId!=null){
			CasCadeScan csc=new CasCadeScan();
			csc.setScan(scan1);
			csc.addPScanKey(Bytes.toString(parentId));
			return e.getEnvironment().getRegion().getScanner(csc.getScan());
		}
		return e.getEnvironment().getRegion().getScanner(scan1);
	}
	
	@Override
	public boolean postScannerNext(
			ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s,
			List<Result> results, int limit, boolean hasMore)
			throws IOException {
		HTableDescriptor hd = e.getEnvironment().getRegion().getTableDesc();
		if (metaIndex == null
				|| metaIndex.getTbIndexNameMap().get(hd.getNameAsString()) == null)
			return hasMore;
		Filter f=null;
		try {
			FilterWrapper fw=ReFelctionUtil.getDynamicObj(s.getClass(), "filter", s);
			if(fw==null)
				return hasMore;
			f = ReFelctionUtil.getDynamicObj(fw.getClass(), "filter", fw);
		} catch (Exception e1) {
			FilterWrapper fw=null;
			try {
				fw = ReFelctionUtil.getDynamicObj(s.getClass().getSuperclass(), "filter", s);
				if(fw==null)
					return hasMore;
				f = ReFelctionUtil.getDynamicObj(fw.getClass(), "filter", fw);
			} catch (Exception e2) {
				LOG.error(e1.getMessage(),e2);
				throw new HbaseIdxScanForOrderException(e2.getMessage());
			}
		}
			if(f instanceof ComplexRowFilter){
				ComplexRowFilter crf=(ComplexRowFilter)f;
				if(crf.getComparator() instanceof ComplexSubstringComparator){
					ComplexSubstringComparator csc=(ComplexSubstringComparator)crf.getComparator();
					if(csc==null)
						return hasMore;
					List<String> tmpOrderList=csc.getListScan().getTmpOrderList();
					reOrder(results,tmpOrderList);
				}
			}
		return hasMore;
	}

	private void reOrder(List<Result> results, List<String> tmpOrderList) {
		if(CollectionUtils.isEmpty(tmpOrderList)||CollectionUtils.isEmpty(results))
			return;
		List<Result> ll=new ArrayList<Result>(tmpOrderList.size());
		x:for(String to:tmpOrderList){
			for(Result r:results){
				if(Bytes.toString(r.getRow()).contains(to)){
					ll.add(r);
					continue x;
				}
			}
		}
		Object[] a=ll.toArray();
        ListIterator<Result> i = results.listIterator();
        for (int j=0; j<a.length; j++) {
            i.next();
            i.set((Result) a[j]);
        }
	}

	private void addIdxCascadeId(MetaScan idxscan, byte[] byteId,
			String cascadeId) {
		if (byteId != null) {
			if (idxscan.getScan().getFilter() instanceof FilterList) {
				FilterList fl = (FilterList) idxscan.getScan().getFilter();
				for (Filter f : fl.getFilters()) {
					if (f instanceof ComplexRowFilter) {
						ComplexRowFilter cr = (ComplexRowFilter) f;
						if (cr.getComparator() instanceof ComplexSubstringComparator) {
							ComplexSubstringComparator csc = (ComplexSubstringComparator) cr
									.getComparator();
							if (csc.getListScan().getListScan() != null) {
								String[] casArray = cascadeId
										.split(Constants.QSPLITTER);
								csc.getListScan()
										.getListScan()
										.add(new ScanOrderedKV(casArray[0],
												casArray[1], CompareOp.EQUAL));
								return;
							}
						}
					}
				}
			}
		}
	}

	private void removeidxScanCascadeId(MetaScan idxscan, String cascadeId) {
		if(cascadeId==null)
			return ;
		if (idxscan.getScan().getFilter() instanceof FilterList) {
			FilterList fl = (FilterList) idxscan.getScan().getFilter();
			for (Filter f : fl.getFilters()) {
				if (f instanceof ComplexRowFilter) {
					ComplexRowFilter cr = (ComplexRowFilter) f;
					if (cr.getComparator() instanceof ComplexSubstringComparator) {
						ComplexSubstringComparator csc = (ComplexSubstringComparator) cr
								.getComparator();
						if (csc.getListScan().getListScan() != null) {
							String[] casArray = cascadeId
									.split(Constants.QSPLITTER);
							for (ScanOrderedKV sokv : csc.getListScan()
									.getListScan()) {
								if (sokv.equals(new ScanOrderedKV(casArray[0],
										casArray[1], CompareOp.EQUAL))) {
									csc.getListScan().getListScan()
											.remove(sokv);
									return;
								}
							}
						}
					}
				}
			}
		}
	}

	private boolean setRangeidxScan(Scan scan, MetaScan idxscan, String familyName, boolean isOrdered,Region region) {
		if(scan.isReversed())
		idxscan.getScan().setStopRow(
				Bytes.toString(scan.getStopRow())
						.replaceFirst(Constants.REGTABLEHBASESTART,
								Constants.REGIDXHBASESTART).getBytes());
		else
		idxscan.getScan().setStopRow(
				Bytes.toString(scan.getStopRow())
				.replaceFirst(Constants.REGTABLEHBASESTOP,
						Constants.REGIDXHBASESTOP).getBytes());
		ComplexRowFilter ccrf=null;
		if(scan.getFilter() instanceof FilterList){
			FilterList fl=(FilterList)scan.getFilter();
			for(Filter f:fl.getFilters()){ 
				if(f instanceof ComplexRowFilter){
					ccrf=(ComplexRowFilter)f;
					break;
				}
			}
		}
		else if(scan.getFilter() instanceof ComplexRowFilter){
			ccrf=(ComplexRowFilter)scan.getFilter();
		}
		if(ccrf==null)
			return true;
		if (!isOrdered||ccrf.isFirstPage()) {
			idxscan.getScan().setStartRow(
					Bytes.toString(scan.getStartRow())
							.replaceFirst(Constants.REGTABLEHBASESTART,
									Constants.REGIDXHBASESTART).getBytes());
			return true;
		}
		else{
			String row=Bytes.toString(scan.getStartRow());
			 String[] ss=null;
			  if(row.contains(Constants.QLIFIERSPLITTER)){
				  String r=row.split(Constants.QLIFIERSPLITTER,2)[1];
				  ss=r.split(Constants.SPLITTER);
			  }
			  else
			   ss=row.split(Constants.SPLITTER);
			  ListScanOrderedKV listScan=new ListScanOrderedKV();
			  List<ScanOrderedKV> list=new ArrayList<ScanOrderedKV>();
			  listScan.setListScan(list);
			  listScan.setSubStr(idxscan.getMi().getIdxName());
			  RowKeyComposition rkc = JSON.parseObject(region.getTableDesc()
						.getValue(familyName), RowKeyComposition.class);
			  for(String s:ss){
				  String[] x=s.split(Constants.QSPLITTER);
				 if(x[0].equals(rkc.getMainHbaseCfPk().getOidQulifierName())){
					 list.add(new ScanOrderedKV(x[0],x[1],CompareOp.EQUAL));
					 break;
				 }
			  }
			try {
				ComplexSubstringComparator csc = new ComplexSubstringComparator(listScan);
				 ComplexRowFilter crf=new ComplexRowFilter(CompareOp.EQUAL, csc);
				   Scan tmpScan=new Scan(); 
				   tmpScan.setStartRow((row.split(Constants.SPLITTER)[0]+Constants.REGIDXHBASESTART).getBytes());
				   tmpScan.setFilter(crf);
				   tmpScan.addColumn(familyName.getBytes(), Constants.IDXPREFIX.getBytes());
				   InternalScanner is = region.getScanner(tmpScan);
					boolean hasMoreRows = false;
					do {
						List<Cell> cells = new ArrayList<Cell>();
						hasMoreRows = is.next(cells);
						if (!cells.isEmpty()) {
							idxscan.getScan().setStartRow(cells.get(0).getRow());
							break;
						}
					} while (hasMoreRows);
					is.close();
			} catch (IOException e) {
				LOG.error(e.getMessage(), e);
				try {
					throw new HbaseScanException(e.getMessage());
				} catch (HbaseScanException e1) {
				}
			}
			return false;
		}
	}

	private Scan buildIndexFilter(ComplexRowFilter sbc, List<byte[]> rowll,
			String tbName, MetaScan idxscan, List<String> scanQulifierList, boolean isOrdered) {
		Scan scan = new Scan();
		scan.setCaching(20);
		scan.addFamily(sbc.getIdxDescfamily().getBytes());
		if (scanQulifierList != null && !scanQulifierList.isEmpty()) {
			for (String s : scanQulifierList) {
				scan.addColumn(sbc.getIdxDescfamily().getBytes(), s.getBytes());
			}
		}
		ComplexRowFilter rf = buildComplexTRowFilter(rowll, idxscan.getMi(),isOrdered);
		if(rowll.size()==1){
			if (null != sbc.getPostScanNext())
				rf.setPostScanNext(sbc.getPostScanNext());
			scan.setFilter(rf);
			return scan;
		}
		if (idxscan.getComplexQulifierCond() == null
				&& idxscan.getSimpleQulifierCond() == null) {
			scan.setFilter(rf);
			if (null != sbc.getPostScanNext())
				rf.setPostScanNext(sbc.getPostScanNext());
			return scan;
		}
		FilterList fl = new FilterList();
		fl.addFilter(rf);
		PartRowKeyString prks = new PartRowKeyString(
				idxscan.getComplexQulifierCond(),
				idxscan.getSimpleQulifierCond(), scanQulifierList);
		prks.buildFliterList(Constants.REGHBASEPREFTIX,
				Constants.REGTABLEHBASEPREFIIX, fl);
		ComplexRowFilter rr = (ComplexRowFilter) fl.getFilters().get(0);
		if (null != sbc.getPostScanNext())
			rr.setPostScanNext(sbc.getPostScanNext());
		scan.setFilter(fl);
		return scan;
	}

	private ComplexRowFilter buildComplexTRowFilter(List<byte[]> rowll,
			MetaIndex mi,boolean isOrdered) {
		if(rowll.size()==1){
			String desctid = Bytes.toString(rowll.get(0)).split(Constants.SPLITTER)[mi.getIdxPos()];
			ComplexRowFilter filter1 = new ComplexRowFilter(
					CompareFilter.CompareOp.EQUAL,new SubstringComparator(desctid));
			return filter1;
		}
		StringBuilder rexStr = new StringBuilder(Constants.REGHBASEPREFTIX);
		int i = 0;
		List<String> tmpOrderList=new ArrayList<String>(rowll.size());
		for (byte[] b : rowll) {
			String desctid = Bytes.toString(b).split(Constants.SPLITTER)[mi
					.getIdxPos()];
			rexStr.append(desctid);
			if(isOrdered)
			tmpOrderList.add(desctid);
			if (i++ < rowll.size() - 1) {
				rexStr.append("|");
			}
		}
		rexStr.append(").*");
		ListScanOrderedKV listScan=new ListScanOrderedKV();
		listScan.setExpr(rexStr.toString());
		if(isOrdered)
		listScan.setTmpOrderList(tmpOrderList);
		ComplexRowFilter filter1=null;
		try {
			filter1 = new ComplexRowFilter(
					CompareFilter.CompareOp.EQUAL, new ComplexSubstringComparator(
							listScan));
		} catch (IOException e) {
			LOG.error(e.getMessage(),e);
			try {
				throw new HbaseScanException(e.getMessage());
			} catch (HbaseScanException e1) {
			}
		}
		return filter1;
	}

	@Override
	public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e,
			Delete delete, WALEdit edit, Durability durability)
			throws IOException {
		HTableDescriptor hd = e.getEnvironment().getRegion().getTableDesc();
		if (checkDelValid(hd))
			return;
		if (checkpostDelete(delete))
			return;
		String familyName = getFamilyNameByDel(delete.getFamilyCellMap()
				.keySet());
		int idxDestPost = fetchPos(familyName, hd.getTableName()
				.getNameAsString(), metaIndex);
		if (idxDestPost == -1)
			return;
		Scan scan = new Scan();
		String[] delRow = Bytes.toString(delete.getRow()).split(
				Constants.SPLITTER);
		scan.setStartRow(new StringBuilder(delRow[0])
				.append(Constants.REGIDXHBASESTART).toString().getBytes());
		scan.setStopRow(new StringBuilder(delRow[0])
				.append(Constants.REGIDXHBASESTOP).toString().getBytes());
		FilterList fl = new FilterList();
		Filter filter = new ComplexRowFilter(CompareFilter.CompareOp.EQUAL,
				new SubstringComparator(delRow[idxDestPost]));
		fl.addFilter(filter);
		fl.addFilter(new FirstKeyOnlyFilter());
		scan.setFilter(fl);
		scan.setCaching(10);
		scan.addColumn(familyName.getBytes(), Constants.IDXPREFIX.getBytes());
		Region region = e.getEnvironment().getRegion();
		InternalScanner is = region.getScanner(scan);
		boolean hasMoreRows = false;
		List<Delete> listDel = new ArrayList<Delete>();
		do {
			List<Cell> cells = new ArrayList<Cell>();
			hasMoreRows = is.next(cells);
			if (!cells.isEmpty()) {
				Delete del = new Delete(cells.get(0).getRow());
				del.setAttribute(Constants.IDXHBASEDEL, "".getBytes());
				del.setDurability(Durability.ASYNC_WAL);
				listDel.add(del);
			}
		} while (hasMoreRows);
		is.close();
		if (!listDel.isEmpty()) {
			HRegion h = (HRegion) region;
			h.batchMutate(listDel.toArray(new Delete[] {}));
		}
//		try {
//			byte[] bb = delete.getAttribute(Constants.HBASEERRORDELETEROW4DEL);
//			if (bb != null) {
//				StringList sl = HbaseUtil.kyroDeSeriLize(bb, StringList.class);
//				if (sl != null && sl.getQulifierList() != null) {
//					List<Delete> ll = new ArrayList<Delete>();
//					op: for (String row : sl.getQulifierList()) {
//						Delete del = new Delete(row.getBytes());
//						del.setAttribute(Constants.IDXHBASEDEL, "".getBytes());
//						Scan sscan = buildDelScan(region, del, familyName);
//						RegionScanner isxRs = region.getScanner(sscan);
//						boolean hasMoreRowss = false;
//						do {
//							List<Cell> cells = new ArrayList<Cell>();
//							hasMoreRows = isxRs.next(cells);
//							if (!cells.isEmpty()) {
//								continue op;
//							}
//						} while (hasMoreRowss);
//						ll.add(del);
//					}
////					HTableInterface hti = pool
////							.getTable(Constants.IDXTABLENAMEPREFIX
////									+ e.getEnvironment().getRegion()
////											.getTableDesc().getNameAsString());
////					hti.delete(ll);
////					hti.close();
//				}
//			}
//		} catch (Exception e1) {
//			LOG.error(e1.getMessage(), e1);
//			throw new HbaseDeleteException(e1.getMessage());
//		}
	}

	public boolean checkDelValid(HTableDescriptor hd) {
		if (metaIndex == null
				|| metaIndex.getTbIndexNameMap().get(hd.getNameAsString()) == null)
			return true;
		if (hd.getNameAsString().startsWith(Constants.IDXTABLENAMEESPREFIX))
			return true;
		return false;
	}

	@Override
	public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e,
			Delete delete, WALEdit edit, Durability durability)
			throws IOException {
		HTableDescriptor hd = e.getEnvironment().getRegion().getTableDesc();
		if (checkDelValid(hd))
			return;
		if (checkpreDelete(delete, e.getEnvironment().getRegion()))
			return;
		try {
			Result r = fetchpreUpdatePut(delete.getRow(), e.getEnvironment()
					.getRegion(), null, null);
			if (r == null || r.isEmpty())
				return;
			String familyName=Bytes.toString(r.getNoVersionMap().keySet().iterator().next());
			if (checkIsIdxLocked(lockedIdxMap, delete, hd, familyName)) {
				throw new HbaseIdxLockedException(hd.getNameAsString()+":"+familyName+" is blocked delete by idx lock");
			}
			String rowPrefix = Bytes.toString(r.getRow()).split(
					Constants.SPLITTER)[0];
			RowKeyComposition rkc = JSON.parseObject(hd.getValue(familyName),
					RowKeyComposition.class);
			for (Cell c : r.listCells()) {
				if(rkc.getFamilyColsNeedIdx().contains(Bytes.toString(c.getQualifier()))){
					EsIdxHbaseType p=new EsIdxHbaseType();
					p.setColumnName(Bytes.toString(c
							.getQualifier()));
					p.setFamilyName(familyName);
					p.setIdxValue(Bytes.toString(c.getValue()));
					p.setMainColumn(rkc.getMainHbaseCfPk().getOidQulifierName());
					p.setOp(Operation.Delete);
					p.setPrefix(rowPrefix);
					p.setRowKey(Bytes.toString(r.getRow()));
					p.setTbName(hd.getTableName().getNameAsString());
					producer.sendMsg(p, p.getRowKey());
				}
			}
//			StringList sl = new StringList(new ArrayList<String>());
//			HTableInterface hti = pool.getTable(Constants.IDXTABLENAMEPREFIX
//					+ e.getEnvironment().getRegion().getTableDesc()
//							.getNameAsString());
//			int i = 0;
//			for (boolean f : hti.existsAll(gets)) {
//				if (f) {
//					sl.getQulifierList().add(
//							Bytes.toString(gets.get(i).getRow()));
//				}
//				i++;
//			}
//			hti.close();
//			delete.setAttribute(Constants.HBASEERRORDELETEROW4DEL,
//					HbaseUtil.kyroSeriLize(sl, -1));
		} catch (Exception e1) {
			LOG.error(e1.getMessage(), e1);
			throw new HbaseDeleteException(e1.getMessage());
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> e,
			Put put, WALEdit edit, Durability durability) throws IOException {
		byte[] xx=put.getAttribute(Constants.HBASEUPDATEROWFORUPDATE);
		if(xx!=null){
			try {
				Map mapList=KryoUtil.kyroDeSeriLize(xx, Map.class);
				handleHbaseTbListPut(mapList);
			} catch (Exception e1) {
				LOG.error(e1.getMessage(),e1);
			}
		}
		byte[] rowk = checkIsUpdatePutForDelete(put);
		if (rowk != null) {
			HRegion h = (HRegion) e.getEnvironment().getRegion();
			Result r = fetchpreUpdatePut(put.getRow(), h, null, null);
			Put ppp = new Put(rowk);
			for (Cell c : r.listCells()) {
				ppp.add(c.getFamily(), c.getQualifier(), c.getValue());
			}
			ppp.setAttribute(Constants.IDXHBASEPUT, "".getBytes());
			ppp.setDurability(Durability.ASYNC_WAL);
			Delete del = new Delete(put.getRow());
			del.setAttribute(Constants.IDXHBASEDEL, "".getBytes());
			del.setDurability(Durability.ASYNC_WAL);
			h.batchMutate(new Delete[] { del });
			h.batchMutate(new Put[] { ppp });
		}
		byte[] bb = put.getAttribute(Constants.HBASEERRORUPDATEROW4DEL);
		if (bb != null) {
			HRegion h = (HRegion) e.getEnvironment().getRegion();
			Delete del = new Delete(bb);
			del.setDurability(Durability.ASYNC_WAL);
			del.setAttribute(Constants.IDXHBASEDEL, "".getBytes());
			h.batchMutate(new Delete[] { del });
		}
	}
}