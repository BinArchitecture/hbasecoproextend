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
package org.apache.hadoop.hbase.coprocessor.observer.handler;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.MetaIndex;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.StringList;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.coprocessor.observer.handler.impl.IndexRegionObserverInsertHandler;
import org.apache.hadoop.hbase.coprocessor.observer.handler.impl.IndexRegionObserverUpHandler;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("deprecation")
public class IndexRegionObserverPutHandler {
	protected static final Log logger = LogFactory
			.getLog(IndexRegionObserverPutHandler.class);
	private static IndexRegionObserverPutHandler instance=new IndexRegionObserverPutHandler();
	public IndexRegionObserverPutHandler(){
		this.mapInstance=new HashMap<Boolean,IndexRegionObserverPutHandler>(2);
		mapInstance.put(false, IndexRegionObserverInsertHandler.getInstance());
		mapInstance.put(true, IndexRegionObserverUpHandler.getInstance());
	}
	public static IndexRegionObserverPutHandler getInstance(){
		return instance;
	}
	private Map<Boolean,IndexRegionObserverPutHandler> mapInstance;
	
	protected Map<String, Cell> buildMultiCell(List<Cell> k) {
		Map<String, Cell> map = new HashMap<String, Cell>();
		for (Cell kk : k) {
			map.put(Bytes.toString(kk.getQualifier()), kk);
		}
		return map;
	}
	
	protected Result fetchpreUpdatePut(byte[] row, Region region) {
		Get get=new Get(row);
		Result r=null;
		try {
			r = region.get(get);
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
		}
		return r;
	}
	
//	protected boolean checkIsUpdatePut(Put put) {
//		return put.getAttribute(Constants.HBASEUPDATEROW)!=null;
//	}
	
	public void handleRegionRow(Put put, String tbName,Map<String, Cell> mapk,String familyName,Region region,Map<StringList, MetaIndex> multiIndexMap){
		mapInstance.get(HbaseUtil.checkIsUpdatePut(put)).handleRegionRow(put, tbName, mapk, familyName, region, multiIndexMap);
	}
	
	@SuppressWarnings("rawtypes")
	public Map<String,List> handleIdxRow(String tbName,Put put,Region region,Map<String, Cell> mapk,String familyName,RowKeyComposition rkc){
		return mapInstance.get(HbaseUtil.checkIsUpdatePut(put)).handleIdxRow(tbName,put,region, mapk, familyName, rkc);
	}
}