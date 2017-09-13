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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbaseReflectionException;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaTableIndex;
import org.apache.hadoop.hbase.client.coprocessor.util.ReFelctionUtil;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.observer.handler.CacadeObserverHandler;
import org.apache.hadoop.hbase.filter.ComplexRowFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterWrapper;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseRegionObserver extends BaseRegionObserver {

	private static final Log LOG = LogFactory.getLog(HBaseRegionObserver.class
			.getName());
	protected boolean checkprePut(Put put) throws IOException {
		return put.getAttribute(Constants.IDXHBASEPUT)!=null;
	}
	
	protected boolean checkIsIdxLocked(Map<String,Map<String,Boolean>> lockedIdxMap,Put put, HTableDescriptor hd, String familyName) {
		if(lockedIdxMap.get(hd.getNameAsString())!=null&&lockedIdxMap.get(hd.getNameAsString()).get(familyName)!=null&&
				lockedIdxMap.get(hd.getNameAsString()).get(familyName)){
			return true;
		}
		return false;
	} 
	
	protected boolean checkIsIdxLocked(Map<String,Map<String,Boolean>> lockedIdxMap,Delete del, HTableDescriptor hd, String familyName) {
		if(lockedIdxMap.get(hd.getNameAsString())!=null&&lockedIdxMap.get(hd.getNameAsString()).get(familyName)!=null&&
				lockedIdxMap.get(hd.getNameAsString()).get(familyName)){
			return true;
		}
		return false;
	} 
	
	protected CacadeObserverHandler getCasCade(String TableName) throws IOException {
		return IndexRegionObserver.getMetaIndex()==null||!IndexRegionObserver.getMetaIndex().getTbIndexNameMap()
				.containsKey(TableName)?null:CacadeObserverHandler.getInstance();
	}
	
	protected boolean checkpreDelete(Delete del,Region region) throws IOException{
		if (checkpostDelete(del))
			return true;
		if (Bytes.toString(del.getRow()).contains(Constants.REGDELHBASEPREFIIX))
			return true;
		return del.getAttribute(Constants.IDXHBASETABLEDEL)==null;
	}
	
	protected boolean checkpostDelete(Delete del) throws IOException {
		return del.getAttribute(Constants.IDXHBASEDEL)!=null;
	}
	
	@SuppressWarnings("deprecation")
	protected Map<String,Object> checkIsPostNext(InternalScanner s, List<Result> results) {
		if(results==null||results.isEmpty())
			return null;
		List<Cell> ll=results.get(0).listCells();
		if(ll==null||ll.isEmpty())
			return null;
		Filter f=null;
		try {
			FilterWrapper fw=ReFelctionUtil.getDynamicObj(s.getClass(), "filter", s);
			if(fw==null)
				return null;
			f = ReFelctionUtil.getDynamicObj(fw.getClass(), "filter", fw);
		} catch (Exception e) {
			FilterWrapper fw=null;
			try {
				fw = ReFelctionUtil.getDynamicObj(s.getClass().getSuperclass(), "filter", s);
				if(fw==null)
					return null;
				f = ReFelctionUtil.getDynamicObj(fw.getClass(), "filter", fw);
			} catch (Exception e2) {
				LOG.error(e2.getMessage(),e2);
				try {
					throw new HbaseReflectionException(e2.getMessage());
				} catch (HbaseReflectionException e1) {
				}
			}
		}
		if(!(f instanceof ComplexRowFilter || f instanceof FilterList)){
			return null;
		}
		ComplexRowFilter rf=null;
		if (f instanceof ComplexRowFilter) {
			rf = (ComplexRowFilter) f;
		} else if (f instanceof FilterList) {
			FilterList fl = (FilterList) f;
			if (fl.getFilters().get(0) instanceof ComplexRowFilter)
				rf = (ComplexRowFilter) fl.getFilters().get(0);
			else
				return null;
		}
		Map<String,Object> map=new HashMap<String,Object>(2);
		map.put(Constants.HBASEFAMILY, Bytes.toString(ll.get(0).getFamily()));
		map.put(Constants.HBASECASCADEMAP, rf.getPostScanNext());
	    return map;
	}
	
	protected ComplexRowFilter getSubIdxRowFilter(Scan scan) {
		if (scan.getFilter() == null
				|| !(scan.getFilter() instanceof ComplexRowFilter || scan.getFilter() instanceof FilterList)) {
			return null;
		}
		ComplexRowFilter rf = null;
		if (scan.getFilter() instanceof ComplexRowFilter) {
			rf = (ComplexRowFilter) scan.getFilter();
		} else if (scan.getFilter() instanceof FilterList) {
			FilterList fl = (FilterList) scan.getFilter();
			if (fl.getFilters().get(0) instanceof ComplexRowFilter)
				rf = (ComplexRowFilter) fl.getFilters().get(0);
			else
				return null;
		}
		if (rf.getIdxDescfamily() == null || rf.getScanRequireCond() == null)
			return null;
		return rf;
	}
	
	protected int fetchPos(String familyName, String tbName, MetaTableIndex metaIndex) {
		return familyName==null||metaIndex.getTbIndexNameMap().get(tbName)==null
				||metaIndex.getTbIndexNameMap().get(tbName).get(familyName)==null?-1:
		  metaIndex.getTbIndexNameMap().get(tbName).get(familyName).getDescpos();
	}

	protected String getFamilyNameByDel(Set<byte[]> fSet) {
		for(byte[] f:fSet){
			if(f!=null){
				return Bytes.toString(f);
			}
		}
		return null;
	}
	
	protected List<Cell> getFamilyCell(Map<byte[],List<Cell>> map) {
		for(byte[] f:map.keySet()){
			if(f!=null){
				return map.get(f);
			}
		}
		return null;
	}
	
	protected boolean checkIsUpdatePut(Put put) {
		return put.getAttribute(Constants.HBASEUPDATEROW)!=null;
	}
	
	protected byte[] checkIsUpdatePutForDelete(Put put) {
		return put.getAttribute(Constants.HBASEUPDATEROWFORDEL);
	}
}