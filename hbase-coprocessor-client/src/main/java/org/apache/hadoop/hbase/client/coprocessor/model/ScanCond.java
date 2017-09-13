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
package org.apache.hadoop.hbase.client.coprocessor.model;


import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.ListScanOrderedKV;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CasCadeScanMap;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.PartRowKeyString;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ComplexRowFilter;
import org.apache.hadoop.hbase.filter.ComplexSubstringComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanCond implements Serializable{
	/**
	 * 
	 */
	private static final Log LOG = LogFactory.getLog(ScanCond.class);
	private static final long serialVersionUID = -2640959366243814114L;
	private String familiName;
	private String[] childFamilyNameArray;
	private int caching=1;
	private CasCadeScanMap casCadeScanMap;
	private String startRow;
	private int limit;
	private String endRow;
	private String scanParentPK;
	private List<String> parentRowKeyList;
	private boolean isReversed;
	private boolean isFirstPage;
	private List<String> scanqulifier;
	public boolean isFirstPage() {
		return isFirstPage;
	}

	public void setFirstPage(boolean isFirstPage) {
		this.isFirstPage = isFirstPage;
	}

	public boolean isReversed() {
		return isReversed;
	}

	public void setReversed(boolean isReversed) {
		this.isReversed = isReversed;
	}
	private PartRowKeyString pr;
	public PartRowKeyString getOrgPr() {
		return orgPr;
	}

	public void setOrgPr(PartRowKeyString orgPr) {
		this.orgPr = orgPr;
	}
	private PartRowKeyString orgPr;
	
	public Scan buildScan(String tableName) {
		Scan scan = new Scan();
        if(!StringUtils.isBlank(familiName))
        scan.addFamily(Bytes.toBytes(familiName));
		scan.setCaching(caching);
		if(!StringUtils.isBlank(startRow))
		scan.setStartRow(startRow.getBytes());
		if(!StringUtils.isBlank(endRow))
		scan.setStopRow(endRow.getBytes());
		scan.setReversed(isReversed);
		if(pr!=null){
			Scan ssc= pr.buildScan(scan,familiName,false,casCadeScanMap, Constants.REGHBASEPREFTIX, Constants.REGTABLEHBASEPREFIIX, true);
			ssc.setReversed(isReversed);
			if(ssc.getFilter() instanceof FilterList){
				FilterList fl=(FilterList)ssc.getFilter();
				for(Filter f:fl.getFilters()){ 
					if(f instanceof ComplexRowFilter){
						ComplexRowFilter crf=(ComplexRowFilter)f;
						crf.setFirstPage(isFirstPage);
						break;
					}
				}
			}
			else if(ssc.getFilter() instanceof ComplexRowFilter){
				ComplexRowFilter crf=(ComplexRowFilter)ssc.getFilter();
				crf.setFirstPage(isFirstPage);
			}
			return ssc;
		}
		else if(CollectionUtils.isNotEmpty(this.parentRowKeyList)){
			PartRowKeyString pr=new PartRowKeyString();
			pr.setComplexScanCond(build(this.parentRowKeyList));
			ComplexRowFilter crf=pr.buildComplexRowFilter(Constants.REGHBASEPREFTIX, Constants.REGTABLEHBASEPREFIIX);
			return buildParentPKScan(scan, crf);
		}
		else if(StringUtils.isNotBlank(this.scanParentPK)){
			ComplexRowFilter crf=buildSimpleRowFilter();
			return buildParentPKScan(scan, crf);
		}
		return null;
	}

	private Scan buildParentPKScan(Scan scan, ComplexRowFilter crf) {
		scan.setFilter(crf);
		if (scanqulifier != null && !scanqulifier.isEmpty()) {
			for (String q : scanqulifier)
				scan.addColumn(familiName.getBytes(), q.getBytes());
		}
		if(null != childFamilyNameArray){
			for(String family:childFamilyNameArray){
				 scan.addFamily(Bytes.toBytes(family));
			}
		}
		return scan;
	}
	
	private List<TreeMap<String, String>> build(List<String> parentRowKeyList) {
		List<TreeMap<String, String>> list=new ArrayList<TreeMap<String, String>>();
		for(String row:parentRowKeyList){
			TreeMap<String, String> map=new TreeMap<String, String>();
			String[] rr=row.split(Constants.QSPLITTER);
			map.put(rr[0], rr[1]);
			list.add(map);
		}
		return list;
	}

	private ComplexRowFilter buildSimpleRowFilter() {
		ListScanOrderedKV listScan=new ListScanOrderedKV();
		 listScan.setSubStr(this.scanParentPK);
		 ComplexSubstringComparator csc=null;
		try {
			csc = new ComplexSubstringComparator(listScan);
		} catch (IOException e) {
			LOG.error(e.getMessage(),e);
		}
		ComplexRowFilter rrf = new ComplexRowFilter(CompareFilter.CompareOp.EQUAL,csc);
		if(casCadeScanMap!=null)
			rrf.setPostScanNext(casCadeScanMap);
		return rrf;
	}
	
	public String getFamiliName() {
		return familiName;
	}
	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public void setFamiliName(String familiName) {
		this.familiName = familiName;
	}
	public int getCaching() {
		return caching;
	}
	public void setCaching(int caching) {
		this.caching = caching;
	}
	public CasCadeScanMap getCasCadeScanMap() {
		return casCadeScanMap;
	}
	public void setCasCadeScanMap(CasCadeScanMap casCadeScanMap) {
		this.casCadeScanMap = casCadeScanMap;
	}
	public PartRowKeyString getPr() {
		return pr;
	}
	public void setPr(PartRowKeyString pr) {
		this.pr = pr;
	}
	public String getStartRow() {
		return startRow;
	}
	public void setStartRow(String startRow) {
		this.startRow = startRow;
	}
	public String getEndRow() {
		return endRow;
	}
	public void setEndRow(String endRow) {
		this.endRow = endRow;
	}

	public String getScanParentPK() {
		return scanParentPK;
	}

	public void setScanParentPK(String scanParentPK) {
		this.scanParentPK = scanParentPK;
	}

	public List<String> getScanqulifier() {
		return scanqulifier;
	}

	public void setScanqulifier(List<String> scanqulifier) {
		this.scanqulifier = scanqulifier;
	}

	public String[] getChildFamilyNameArray() {
		return childFamilyNameArray;
	}

	public void setChildFamilyNameArray(String[] childFamilyNameArray) {
		this.childFamilyNameArray = childFamilyNameArray;
	}

	public List<String> getParentRowKeyList() {
		return parentRowKeyList;
	}

	public void setParentRowKeyList(List<String> parentRowKeyList) {
		this.parentRowKeyList = parentRowKeyList;
	}
}