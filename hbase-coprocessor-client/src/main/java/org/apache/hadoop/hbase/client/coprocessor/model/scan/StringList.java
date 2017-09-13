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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.HbaseDataType;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseStringUtil;

public class StringList implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -2985303353166185440L;
	private List<String> qulifierList;
	private List<HbaseDataType> hbasedataList;
	public StringList(List<String> qulifierList){
		this.qulifierList=buildqulifierList(qulifierList);
	}
	
	private List<String> buildqulifierList(List<String> qulifierList) {
		if(CollectionUtils.isNotEmpty(qulifierList)){
			List<String> qulifierll=new ArrayList<String>();
			for(String q:qulifierList){
				qulifierll.add(HbaseStringUtil.formatStringOrginal(q));
			}
			return qulifierll;
		}
		return null;
	}
	public StringList(){
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((hbasedataList == null) ? 0 : hbasedataList.hashCode());
		result = prime * result
				+ ((qulifierList == null) ? 0 : qulifierList.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StringList other = (StringList) obj;
		if (hbasedataList == null) {
			if (other.hbasedataList != null)
				return false;
		} else if (!hbasedataList.equals(other.hbasedataList))
			return false;
		if (qulifierList == null) {
			if (other.qulifierList != null)
				return false;
		} else if (!qulifierList.equals(other.qulifierList))
			return false;
		return true;
	}
	public List<String> getQulifierList() {
		return qulifierList;
	}

	public void setQulifierList(List<String> qulifierList) {
		this.qulifierList = buildqulifierList(qulifierList);
	}

	public boolean contain(String s) {
		if (s == null || qulifierList == null)
			return false;
		TreeSet<String> qq = buildStringSet();
		for (String ss : qq) {
			if (ss.equals(s))
				return true;
		}
		return false;
	}

	public boolean contain(String[] s) {
		if (s == null || qulifierList == null)
			return false;
		TreeSet<String> qq = buildStringSet();
		q: for (String ss : s) {
			for (String q : qq) {
				if (ss.equals(q))
					continue q;
			}
			return false;
		}
		return true;
	}

	public TreeSet<String> buildStringSet() {
		TreeSet<String> qq=new TreeSet<String>();
		qq.addAll(qulifierList);
		if(hbasedataList!=null){
			for(HbaseDataType hdt:hbasedataList)
				qq.add(hdt.getQulifier());
		}
		return qq;
	}
	
	public boolean contain(List<HbaseDataType> h) {
		if (h == null || hbasedataList == null)
			return false;
		qq: for (HbaseDataType hh : h) {
			for (HbaseDataType q : hbasedataList) {
				if (hh.equals(q))
					continue qq;
			}
			return false;
		}
		String s1=generateString(h);
		String s2=generateString(hbasedataList);
		return s2.indexOf(s1)!=-1;
	}

	private String generateString(List<HbaseDataType> list) {
		StringBuilder sb=new StringBuilder("");
		for(HbaseDataType h:list)
			sb.append(h.getQulifier());
		return sb.toString();
	}
	
	public boolean isMulti() {
		return qulifierList!=null&&qulifierList.size()>1;
	}
	public List<HbaseDataType> getHbasedataList() {
		return hbasedataList;
	}
	public void setHbasedataList(List<HbaseDataType> hbasedataList) {
		this.hbasedataList = hbasedataList;
	}
}