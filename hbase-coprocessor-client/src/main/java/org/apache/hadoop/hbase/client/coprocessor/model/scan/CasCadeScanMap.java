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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.ListScanOrderedKV;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ComplexSubstringComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ComplexRowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class CasCadeScanMap implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -8120760875253487619L;
	private static final Log LOG = LogFactory.getLog(CasCadeScanMap.class
			.getName());
	private Map<String,CasCadeScanMap> mapScan;
	private List<ScanOrderedKV> simpleScanCond;
	public List<ScanOrderedKV> getSimpleScanCond() {
		return simpleScanCond;
	}

	public void setSimpleScanCond(List<ScanOrderedKV> simpleScanCond) {
		this.simpleScanCond = simpleScanCond;
	}

	public synchronized CasCadeScanMap put(String key,List<ScanOrderedKV> simpleScanCond) {
		if (mapScan == null)
			mapScan = new HashMap<String, CasCadeScanMap>(2);
		CasCadeScanMap casCadeScanMap=new CasCadeScanMap();
		mapScan.put(key,casCadeScanMap);
		casCadeScanMap.setSimpleScanCond(simpleScanCond);
		return casCadeScanMap;
	}
	
	public Map<String,CasCadeScan> buildScan(int descPos,byte[] row) {
		if(mapScan==null)
			return null;
		Map<String,CasCadeScan> scanMap=new HashMap<String,CasCadeScan>();
		String rowS=Bytes.toString(row);
		for(String s:mapScan.keySet()){
			CasCadeScanMap cmap=mapScan.get(s);
			if(cmap!=null)
			scanMap.put(s,cmap.buildScan(s,cmap.getSimpleScanCond(),rowS,descPos));
		}
		return scanMap;
	}
	
	private CasCadeScan buildScan(String familyName,List<ScanOrderedKV> listScan, String rowS, int descPos) {
		CasCadeScan casCadeScan=new CasCadeScan();
		Scan scan=new Scan();
		scan.addFamily(Bytes.toBytes(familyName));
		scan.setCaching(10);
		String[] prefixt=rowS.split(Constants.SPLITTER);
		String casCadeId=prefixt[descPos];
		buildDetailScan(familyName,scan,listScan,casCadeId,prefixt[0]);
		casCadeScan.setScan(scan);
		if(mapScan!=null){
			buildScanHandler(casCadeScan, mapScan,rowS,casCadeId);
		}
		return casCadeScan;
	}

	private void buildDetailScan(String familyName, Scan scan, List<ScanOrderedKV> listScan, String casCadeId,String prefix) {
		FilterList fl=new FilterList();
		if(listScan!=null){
			ListScanOrderedKV ll=new ListScanOrderedKV();
			ll.setListScan(listScan);
			ll.setSubStr(casCadeId);
			ComplexSubstringComparator csc=null;
			try {
				csc = new ComplexSubstringComparator(ll);
			} catch (IOException e) {
				LOG.error(e.getMessage(),e);
			}
			ComplexRowFilter rf=new ComplexRowFilter(CompareFilter.CompareOp.EQUAL,csc);
			rf.setIdxDescfamily(familyName);
			PartRowKeyString pr=new PartRowKeyString(null,listScan,null);
			rf.setScanRequireCond(pr);
			fl.addFilter(rf);
		}
		fl.addFilter(new ComplexRowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(casCadeId)));
		scan.setFilter(fl);
		scan.setStartRow(new StringBuilder(prefix).append(Constants.REGTABLEHBASESTART).toString().getBytes());
		scan.setStopRow(new StringBuilder(prefix).append(Constants.REGTABLEHBASESTOP).toString().getBytes());
	}

	private void buildScanHandler(CasCadeScan casCadeScan,
			final Map<String,CasCadeScanMap> mapScan,String rowS, String casCadeId) {
		Map<String, CasCadeScan> map=new HashMap<String, CasCadeScan>(1);
		for(String key:mapScan.keySet()){
			CasCadeScanMap cmap=mapScan.get(key);
			CasCadeScan tmpcasCadeScan=new CasCadeScan();
			Scan scan=new Scan();
			scan.addFamily(Bytes.toBytes(key));
			scan.setCaching(10);
			String[] prefixt=rowS.split(Constants.SPLITTER);
			buildDetailScan(key,scan,cmap.getSimpleScanCond(),casCadeId,prefixt[0]);
			tmpcasCadeScan.setScan(scan);
			map.put(key, tmpcasCadeScan);
			if(cmap.getMapScan()!=null)
			buildScanHandler(tmpcasCadeScan,cmap.getMapScan(),rowS,casCadeId);
		}
		casCadeScan.setCasCadeScanMap(map);
	}
	
	public Map<String, CasCadeScanMap> getMapScan() {
		return mapScan;
	}

	public void setMapScan(Map<String, CasCadeScanMap> mapScan) {
		this.mapScan = mapScan;
	}
}
