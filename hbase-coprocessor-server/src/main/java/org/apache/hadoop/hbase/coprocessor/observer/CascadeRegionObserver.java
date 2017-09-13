/**
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
package org.apache.hadoop.hbase.coprocessor.observer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbaseCasCadeScanException;
import org.apache.hadoop.hbase.client.coprocessor.model.CascadeResult;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaFamilyPosIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CasCadeScan;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CasCadeScanMap;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.observer.handler.CacadeObserverHandler;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("deprecation")
public class CascadeRegionObserver extends HBaseRegionObserver {
	protected static final Log logger = LogFactory
			.getLog(CascadeRegionObserver.class);
	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> e,
			Put put, WALEdit edit, Durability durability) throws IOException {
		if(checkprePut(put)||checkIsUpdatePut(put))
			return;
		HTableDescriptor hd=e.getEnvironment().getRegion().getTableDesc();
		CacadeObserverHandler bol=getCasCade(hd.getNameAsString());
		if(bol==null)
			return;
		List<Put> list=bol.buildPutList(put);
		if(list==null){
			return;
		}
		HRegion h = (HRegion) e.getEnvironment().getRegion();
		h.batchMutate(list.toArray(new Put[] {}));
	}

	@Override
	public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e,
			Delete delete, WALEdit edit, Durability durability)
			throws IOException {
		if(checkpostDelete(delete))
			return;
		HTableDescriptor hd=e.getEnvironment().getRegion().getTableDesc();
		CacadeObserverHandler bol=getCasCade(hd.getNameAsString());
		if(bol==null)
			return;
		String familyName = getFamilyNameByDel(delete.getFamilyCellMap().keySet());
		int idxDestPost=fetchPos(familyName,hd.getTableName().getNameAsString(),IndexRegionObserver.getMetaIndex());
		if(idxDestPost==-1)
			return;
		Scan scan=bol.buildDeleteList(delete,idxDestPost);
		InternalScanner is=e.getEnvironment().getRegion().getScanner(scan);
		boolean hasMoreRows=false;
		List<Delete> listDel=new ArrayList<Delete>();
		do {
			List<Cell> cells=new ArrayList<Cell>();
			hasMoreRows = is.next(cells);
			if(!cells.isEmpty()){
				Delete del=new Delete(cells.get(0).getRow());
				del.addFamily(cells.get(0).getFamily());
				listDel.add(del);
			}
		} while (hasMoreRows);
			is.close();
		if(listDel.isEmpty()){
			return;
		}
		HRegion h = (HRegion) e.getEnvironment().getRegion();
		h.batchMutate(listDel.toArray(new Delete[] {}));
	}

	@Override
	public boolean postScannerNext(
			ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s,
			List<Result> results, int limit, boolean hasMore)
			throws IOException {
			HTableDescriptor desc=e.getEnvironment().getRegion().getTableDesc();
			CacadeObserverHandler bol=getCasCade(desc.getNameAsString());
			if(bol!=null){
				Map<String,Object> objMap=checkIsPostNext(s, results);
				if(null==objMap)
					return hasMore;
				Map<String, MetaFamilyPosIndex> mmp=IndexRegionObserver.getMetaIndex().getTbIndexNameMap().get(desc.getNameAsString());
				if(mmp==null)
					return hasMore;
				MetaFamilyPosIndex mpi=mmp.get((String)objMap.get(Constants.HBASEFAMILY));
				if(mpi==null)
					return hasMore;
				for(Result r:results){
					Map<String,CasCadeScan> scan=bol.buildScan(r.getRow(),mpi.getDescpos(),(CasCadeScanMap)objMap.get(Constants.HBASECASCADEMAP));
					if(scan==null)
						return hasMore;
					String cascadeId=Bytes.toString(r.getRow()).split(Constants.SPLITTER)[mpi.getDescpos()];
					CascadeResult cr=new CascadeResult(r);
					buildCell0(e, scan,cr,cascadeId,null);
					cr.build();
				}
			}
			return hasMore;
	}

	private void buildCell0(ObserverContext<RegionCoprocessorEnvironment> e,
			Map<String, CasCadeScan> scan, CascadeResult r, String cascadeId,String parentCascadeId) {
		Map<String,List<CascadeResult>> map=new HashMap<String,List<CascadeResult>>();
		for(String familyKey:scan.keySet()){
		try {
			CasCadeScan csc=scan.get(familyKey);
			Region region=e.getEnvironment().getRegion();
			if(parentCascadeId!=null){
				csc.addPScanKey(parentCascadeId);
				csc.getScan().setAttribute(Constants.HBASEPARENTKEY,parentCascadeId.getBytes());
			}
			InternalScanner is=region.getScanner(csc.getScan());
			csc.getScan().setAttribute(Constants.HBASEMAINKEY,cascadeId.getBytes());
			RegionScanner rs=(RegionScanner)is;
			rs = region.getCoprocessorHost().postScannerOpen(csc.getScan(), rs);
			boolean hasMoreRows = false;
			List<CascadeResult> ll=new ArrayList<CascadeResult>();
			do {
				List<Cell> cells=new ArrayList<Cell>();
				long tStart = System.currentTimeMillis(); 
				hasMoreRows = rs.next(cells);
				long tEnd = System.currentTimeMillis(); 
				if(!cells.isEmpty()){
				logger.debug(tEnd - tStart + "millions");
				Result rr=Result.create(cells);
				CascadeResult cr=new CascadeResult(rr);
				Map<String, MetaFamilyPosIndex> mmp=IndexRegionObserver.getMetaIndex().getTbIndexNameMap().get(region.getTableDesc().getNameAsString());
				String currCascadeId=null;
				if(mmp!=null){
				MetaFamilyPosIndex mpi=mmp.get(familyKey);				
				currCascadeId=Bytes.toString(rr.getRow()).split(Constants.SPLITTER)[mpi.getDescpos()];
				}
				if(csc.getCasCadeScanMap()!=null){
					buildCell0(e,csc.getCasCadeScanMap(),cr,cascadeId,currCascadeId);
				}
				ll.add(cr);
				}
			} while (hasMoreRows);
			is.close();
			map.put(familyKey, ll);
		} catch (IOException e1) {
		  logger.error(e1.getMessage(),e1);
		  try {
			throw new HbaseCasCadeScanException(e1.getMessage());
		} catch (HbaseCasCadeScanException e2) {
		}
		}
	}
    r.setRmap(map);
	}
}
