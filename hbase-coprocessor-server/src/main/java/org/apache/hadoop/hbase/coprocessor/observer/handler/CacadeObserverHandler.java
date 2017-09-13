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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CasCadeListCell;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CasCadeScan;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CasCadeScanMap;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CascadeCell;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ComplexRowFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class CacadeObserverHandler {
	protected static final Log logger = LogFactory
			.getLog(CacadeObserverHandler.class);
	private static CacadeObserverHandler instance=new CacadeObserverHandler();
	private CacadeObserverHandler(){}
	public static CacadeObserverHandler getInstance(){
		return instance;
	}
	public Map<String,CasCadeScan> buildScan(byte[] row,int descPos,CasCadeScanMap cmap){
		try {
			if(cmap==null)
				return null;
			return cmap.buildScan(descPos, row);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
			return null;
		}
	}
	
	@SuppressWarnings("deprecation")
	public List<Put> buildPutList(Put put) {
		List<Put> list=null;
		try {
			if(put.getAttribute(Constants.HBASECASCADEROW)==null)
				return null;
			CasCadeListCell mapCell=HbaseUtil.kyroDeSeriLize(put.getAttribute(Constants.HBASECASCADEROW), CasCadeListCell.class);
			list = new ArrayList<Put>();
			for (String k : mapCell.getListCell().keySet()) {
				for (CascadeCell cell : mapCell.getListCell().get(k)) {
					Put p=new Put(cell.getRow());
					for(String kk:cell.getQulifyerValueMap().keySet()){
						p.add(cell.getFamily(), Bytes.toBytes(kk),Bytes.toBytes(cell.getQulifyerValueMap().get(kk)));
					}
					list.add(p);
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
		return list;
	}
	
	public Scan buildDeleteList(Delete delete,int familyDestPos) {
		String rowkey=Bytes.toString(delete.getRow());
		String[] ss=rowkey.split(Constants.SPLITTER);
		Scan scan=new Scan();
		FilterList fl=new FilterList();
		Filter filter = new ComplexRowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(ss[familyDestPos]));
		fl.addFilter(filter);
		fl.addFilter(new FirstKeyOnlyFilter());
		scan.setFilter(fl);
		scan.setCaching(100);
		scan.setStartRow(new StringBuilder(ss[0]).append(Constants.REGTABLEHBASESTART).toString().getBytes());
		scan.setStopRow(new StringBuilder(ss[0]).append(Constants.REGTABLEHBASESTOP).toString().getBytes());
		return scan;
	}

}
