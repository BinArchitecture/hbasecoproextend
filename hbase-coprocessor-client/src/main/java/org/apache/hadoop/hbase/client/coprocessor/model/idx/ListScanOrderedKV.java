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
import java.util.List;

import org.apache.hadoop.hbase.client.coprocessor.model.scan.ScanOrderedKV;

public class ListScanOrderedKV implements Serializable{
/**
	 * 
	 */
	private static final long serialVersionUID = 5606506168518627624L;

	public List<ScanOrderedKV> getListScan() {
		return listScan;
	}

	public void setListScan(List<ScanOrderedKV> listScan) {
		this.listScan = listScan;
	}

private List<ScanOrderedKV> listScan;
private List<HbaseDataType> orderBy;
private String subStr="";
private String expr="";
private List<String> tmpOrderList;
public List<String> getTmpOrderList() {
	return tmpOrderList;
}

public void setTmpOrderList(List<String> tmpOrderList) {
	this.tmpOrderList = tmpOrderList;
}

public String getExpr() {
	return expr;
}

public void setExpr(String expr) {
	this.expr = expr;
}

@Override
public boolean equals(Object paramObject) {
	if(paramObject==null)
		return false;
	if(!(paramObject instanceof ListScanOrderedKV))
		return false;
	ListScanOrderedKV lok=(ListScanOrderedKV)paramObject;
	if(lok.getListScan().size()!=listScan.size())
		return false;
	if(!lok.getSubStr().equals(subStr))
		return false;
	if(!lok.getExpr().equals(expr))
		return false;
	for(ScanOrderedKV sk:listScan){
		boolean mark=false;
		for(ScanOrderedKV s:lok.getListScan()){
			if(sk.equals(s)){
				mark=true;
				break;
			}
		}
		if(!mark)
			return false;
	}
	return true;
}

public String getSubStr() {
	return subStr;
}

public void setSubStr(String subStr) {
	this.subStr = subStr;
}

public List<HbaseDataType> getOrderBy() {
	return orderBy;
}

public void setOrderBy(List<HbaseDataType> orderBy) {
	this.orderBy = orderBy;
}

}
