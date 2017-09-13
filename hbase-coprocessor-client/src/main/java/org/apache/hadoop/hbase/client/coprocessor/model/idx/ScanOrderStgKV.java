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
package org.apache.hadoop.hbase.client.coprocessor.model.idx;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.EsIdxHbaseType;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.ScanOrderedKV;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import com.lppz.elasticsearch.query.BoolSearchQuery;
import com.lppz.elasticsearch.query.Operator;
import com.lppz.elasticsearch.query.SearchQuery;
import com.lppz.elasticsearch.query.fielditem.RangeItem;
import com.lppz.elasticsearch.query.fielditem.TermKvItem;
import com.lppz.elasticsearch.search.SearchCondition;

public class ScanOrderStgKV implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1764314518718816301L;
	private ScanOrderedKV minsok;
	private ScanOrderedKV maxsok;
	private TreeSet<String> complexScanOrderList;

	public ScanOrderStgKV() {
	}

	public ScanOrderStgKV(ScanOrderedKV minsok, ScanOrderedKV maxsok) {
		this.minsok = minsok;
		this.maxsok = maxsok;
	}

	public List<ScanOrderStgKV> buildComplexList() {
		List<ScanOrderStgKV> list = new ArrayList<ScanOrderStgKV>(1);
		if (CollectionUtils.isEmpty(complexScanOrderList)) {
			list.add(this);
			return list;
		}
		for (String s : complexScanOrderList) {
			list.add(new ScanOrderStgKV(new ScanOrderedKV(minsok.getQulifier(),
					s, CompareOp.EQUAL), new ScanOrderedKV(
					minsok.getQulifier(), s, CompareOp.EQUAL)));
		}
		return list;
	}

	public boolean isMainId(RowKeyComposition rkc){
		return rkc.getMainHbaseCfPk().getOidQulifierName().equals(minsok.getQulifier());
	}
	
	public String buildPrefixByMainId(){
		return HbaseUtil.buildPrefixByMainId(minsok.getValue());
	}
	
	public void buildScanRange(Scan scan, String prefix, String suffix) {
		if (scan == null)
			return;
		if (scan.isReversed()) {
			if (minsok != null)
				scan.setStopRow(new StringBuilder(minsok
						.buildIdxPrefix(Constants.QLIFIERSPLITTER))
						.append(prefix).toString().getBytes());
			if (maxsok != null)
				scan.setStartRow(new StringBuilder(maxsok
						.buildIdxPrefix(Constants.QLIFIERSPLITTER))
						.append(suffix).toString().getBytes());
		} else {
			if (minsok != null)
				scan.setStartRow(new StringBuilder(minsok
						.buildIdxPrefix(Constants.QLIFIERSPLITTER))
						.append(prefix).toString().getBytes());
			if (maxsok != null)
				scan.setStopRow(new StringBuilder(maxsok
						.buildIdxPrefix(Constants.QLIFIERSPLITTER))
						.append(suffix).toString().getBytes());
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((maxsok == null) ? 0 : maxsok.hashCode());
		result = prime * result + ((minsok == null) ? 0 : minsok.hashCode());
		return result;
	}

	public SearchCondition buildSearchCond(String tbName,String familiName, boolean reversed) {
		SearchQuery searchQuery=new SearchQuery();
		searchQuery.setSearchQueryList(new ArrayList<SearchQuery>());
		BoolSearchQuery bsq=new BoolSearchQuery(Operator.AND);
		TermKvItem tf=new TermKvItem();
		tf.setTermField("familyName");
		tf.setTermValue(familiName);
		bsq.addFileItem(tf);
		searchQuery.getSearchQueryList().add(bsq);
		SearchCondition sc=new SearchCondition(searchQuery,Constants.IDXTABLENAMEESPREFIX+tbName+"-*",new String[]{EsIdxHbaseType.class.getName()});
		if(minsok!=null){
			TermKvItem tki=new TermKvItem();
			tki.setTermField("columnName");
			tki.setTermValue(minsok.getQulifier());
			bsq.addFileItem(tki);
			if(minsok.getOp().equals(CompareOp.EQUAL)){
				TermKvItem tt=new TermKvItem();
				tt.setTermField("idxValue");
				tt.setTermValue(minsok.getValue());
				bsq.addFileItem(tt);
			}
			else{
				RangeItem ri=new RangeItem();
				ri.setTermField("idxValue");
				if(!reversed)
				ri.setGeStr(minsok.getValue());
				else
				ri.setLeStr(minsok.getValue());
				bsq.addFileItem(ri);
			}
		}
		if(maxsok!=null){
			if(!minsok.getOp().equals(CompareOp.EQUAL)){
			TermKvItem tki=new TermKvItem();
			tki.setTermField("columnName");
			tki.setTermValue(maxsok.getQulifier());
			bsq.addFileItem(tki);
			RangeItem ri=new RangeItem();
			ri.setTermField("idxValue");
			if(!reversed)
			ri.setLeStr(maxsok.getValue());
			else
			ri.setGeStr(maxsok.getValue());	
			bsq.addFileItem(ri);
			}
		}
		return sc;
	}
	
	public SearchCondition buildSearchCondFast(String tbName,String familiName, boolean reversed) {
		SearchQuery searchQuery=new SearchQuery();
		searchQuery.setSearchQueryList(new ArrayList<SearchQuery>());
		BoolSearchQuery bsq=new BoolSearchQuery(Operator.AND);
//		TermKvItem tf=new TermKvItem();
//		tf.setTermField("familyName");
//		tf.setTermValue(familiName);
//		bsq.addFileItem(tf);
		searchQuery.getSearchQueryList().add(bsq);
		String column=buildColumn();
		String idxName=HbaseUtil.buildEsIdx(tbName, familiName, column, "*");
		SearchCondition sc=new SearchCondition(searchQuery,idxName,new String[]{EsIdxHbaseType.class.getName()});
		if(minsok!=null){
//			TermKvItem tki=new TermKvItem();
//			tki.setTermField("columnName");
//			tki.setTermValue(minsok.getQulifier());
//			bsq.addFileItem(tki);
			RangeItem ri=new RangeItem();
			ri.setTermField("idxValue");
			if(!reversed)
				ri.setGeStr(minsok.getValue());
			else
				ri.setLeStr(minsok.getValue());
			bsq.addFileItem(ri);
		}
		if(maxsok!=null){
//			TermKvItem tki=new TermKvItem();
//			tki.setTermField("columnName");
//			tki.setTermValue(maxsok.getQulifier());
//			bsq.addFileItem(tki);
			RangeItem ri=new RangeItem();
			ri.setTermField("idxValue");
			if(!reversed)
				ri.setLeStr(maxsok.getValue());
			else
				ri.setGeStr(maxsok.getValue());	
			bsq.addFileItem(ri);
		}
		return sc;
	}
	
	private String buildColumn() {
		return minsok!=null?minsok.getQulifier():maxsok.getQulifier();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ScanOrderStgKV other = (ScanOrderStgKV) obj;
		if (maxsok == null) {
			if (other.maxsok != null)
				return false;
		} else if (!maxsok.equals(other.maxsok))
			return false;
		if (minsok == null) {
			if (other.minsok != null)
				return false;
		} else if (!minsok.equals(other.minsok))
			return false;
		return true;
	}

	public ScanOrderedKV getMinsok() {
		return minsok;
	}

	public void setMinsok(ScanOrderedKV minsok) {
		this.minsok = minsok;
	}

	public ScanOrderedKV getMaxsok() {
		return maxsok;
	}

	public void setMaxsok(ScanOrderedKV maxsok) {
		this.maxsok = maxsok;
	}

	public TreeSet<String> getComplexScanOrderList() {
		return complexScanOrderList;
	}

	public void setComplexScanOrderList(TreeSet<String> complexScanOrderList) {
		this.complexScanOrderList = complexScanOrderList;
	}
}