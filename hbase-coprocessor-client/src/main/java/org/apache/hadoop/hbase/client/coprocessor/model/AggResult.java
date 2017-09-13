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

import java.math.BigDecimal;
import java.util.Date;
import org.apache.hadoop.hbase.client.coprocessor.agg.AggHandlerProxy.AggEnum;
import org.apache.hadoop.hbase.client.coprocessor.model.groupby.Std;

public class AggResult {
	private BigDecimal stgResult;
	private String stgResultString;
	private Date stgResultDate;
	private Integer rowCountResult;
	private Std listAvg;
	
	public Object buildValue(AggEnum type,ClassEnum classEnum){
		if(AggEnum.avg.equals(type)){
			if(listAvg==null)
				return null;
			return listAvg.buildAvgNumber();
		}
		if(AggEnum.count.equals(type))
			return rowCountResult;
		if(AggEnum.sum.equals(type))
			return stgResult;
		if(classEnum==null)
			return null;
		if(ClassEnum.Date.equals(classEnum))
			return stgResultDate;
		if(ClassEnum.Double.equals(classEnum))
			return stgResult;
		if(ClassEnum.String.equals(classEnum))
			return stgResultString;
		return null;
	}
	
	public BigDecimal getStgResult() {
		return stgResult;
	}
	public void setStgResult(BigDecimal stgResult) {
		this.stgResult = stgResult;
	}
	public String getStgResultString() {
		return stgResultString;
	}
	public void setStgResultString(String stgResultString) {
		this.stgResultString = stgResultString;
	}
	public Date getStgResultDate() {
		return stgResultDate;
	}
	public void setStgResultDate(Date stgResultDate) {
		this.stgResultDate = stgResultDate;
	}
	public Integer getRowCountResult() {
		return rowCountResult;
	}
	public void setRowCountResult(Integer rowCountResult) {
		this.rowCountResult = rowCountResult;
	}
	public Std getListAvg() {
		return listAvg;
	}
	public void setListAvg(Std listAvg) {
		this.listAvg = listAvg;
	}
}
