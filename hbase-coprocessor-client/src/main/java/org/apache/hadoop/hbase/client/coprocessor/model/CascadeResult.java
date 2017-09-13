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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.CascadeCell;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.ScanCascadeResult;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseStringUtil;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.util.Bytes;

public class CascadeResult implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -7850409653323091754L;
	private static final Log logger = LogFactory
			.getLog(HbaseUtil.class);
	private Result r;
	private Map<String, List<CascadeResult>> rmap;
	private ScanCascadeResult scr;
	public CascadeResult(){
	}
	public CascadeResult(Result r){
		this.r=r;
	}
	@SuppressWarnings("deprecation")
	public void build() {
		if (this.r.rawCells() == null || this.r.rawCells().length == 0)
			return;
		ScanCascadeResult scr = build(r);
		build(this, scr);
		Cell[] cc = r.rawCells();
		byte[] family = cc[0].getFamily();
		byte[] qualifier = cc[0].getQualifier();
		byte[] row = cc[0].getRow();
		byte[] value;
		try {
			value = HbaseUtil.kyroSeriLize(scr,-1);
			cc[0] = CellUtil.createCell(row, family, qualifier,
					HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode(),
					value);
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
		}
	}
	
	@SuppressWarnings("deprecation")
	public void debuild() {
		if (r.rawCells() == null || r.rawCells().length == 0)
			return;
		Cell cell = r.rawCells()[0];
		try {
			this.scr =HbaseUtil.kyroDeSeriLize(cell.getValue(), ScanCascadeResult.class);
			if(scr!=null){
				if(scr.getSource()!=null&&scr.getSource().getQulifyerValueMap()!=null){
					for(String key:scr.getSource().getQulifyerValueMap().keySet()){
						String value=scr.getSource().getQulifyerValueMap().get(key);
						if(value!=null)
						scr.getSource().getQulifyerValueMap().put(key,HbaseStringUtil.unformatStringVal(value));
					}
				}
				
			}
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
			this.scr=this.build(r);
		}
	}

	public ScanCascadeResult getScr() {
		return scr;
	}

	public void build(CascadeResult r, ScanCascadeResult sr) {
		if (r.getRmap() != null) {
			for (String s : r.getRmap().keySet()) {
				List<ScanCascadeResult> cc = new ArrayList<ScanCascadeResult>();
				for (CascadeResult rr : r.getRmap().get(s)) {
					ScanCascadeResult ssr = build(rr.getR());
					cc.add(ssr);
					build(rr, ssr);
				}
				sr.getCascadeMap().put(s, cc);
			}
		}
	}

	@SuppressWarnings("deprecation")
	public ScanCascadeResult build(Result r) {
		ScanCascadeResult sc = new ScanCascadeResult();
		CascadeCell source = new CascadeCell();
		sc.setSource(source);
		List<Cell> listCell = r.listCells();
		if (listCell == null || listCell.isEmpty())
			return sc;
		source.setFamily(listCell.get(0).getFamily());
		source.setRow(listCell.get(0).getRow());
		for (Cell cell : listCell) {
			String key=null;
			String value=null;
			try {
				key = Bytes.toString(cell.getQualifier());
				value = HbaseStringUtil.unformatStringVal(Bytes.toString(cell.getValue()));
				source.getQulifyerValueMap().put(key,value);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return sc;
	}

	public Map<String, List<CascadeResult>> getRmap() {
		return rmap;
	}

	public void setRmap(Map<String, List<CascadeResult>> rmap) {
		this.rmap = rmap;
	}
	public Result getR() {
		return r;
	}
}