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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.AggGroupByHandlerProxy.AggGroupByEnum;
import org.apache.hadoop.hbase.client.coprocessor.model.groupby.Avg;
import org.apache.hadoop.hbase.client.coprocessor.model.groupby.Std;

public class AggGroupResult {
	private Map<String, Double> stgResult;
	private Map<String, String> stgResultString;
	private Map<String, Date> stgResultDate;
	private Map<String, Integer> rowCountResult;
	private List<Avg> listAvg;
	
	public Object buildValue(AggGroupByEnum type,ClassEnum classEnum){
		if(AggGroupByEnum.avg.equals(type)){
			if(listAvg==null)
				return null;
			Map<String, Std> map=new HashMap<String, Std>();
			for(Avg avg:listAvg){
				for(String s:avg.getMm().keySet()){
					Std stb=map.get(s);
					Std std=avg.getMm().get(s);
					if(stb==null){
						stb=std;
						map.put(s, stb);
						continue;
					}
					map.put(s, stb.add(std));
				}
			}
			Map<String, Double> mapDouble=new HashMap<String, Double>(map.size());
			for(String ss:map.keySet()){
				mapDouble.put(ss, map.get(ss)==null?0d:map.get(ss).buildAvgNumber());
			}
			return mapDouble;
		}
		if(AggGroupByEnum.count.equals(type))
			return rowCountResult;
		if(AggGroupByEnum.sum.equals(type))
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
	
	public Map<String, Double> getStgResult() {
		return stgResult;
	}

	public void setStgResult(Map<String, Double> stgResult) {
		this.stgResult = stgResult;
	}

	public Map<String, Integer> getRowCountResult() {
		return rowCountResult;
	}

	public void setRowCountResult(Map<String, Integer> rowCountResult) {
		this.rowCountResult = rowCountResult;
	}

	public List<Avg> getListAvg() {
		return listAvg;
	}

	public void setListAvg(List<Avg> listAvg) {
		this.listAvg = listAvg;
	}

	public Map<String, String> getStgResultString() {
		return stgResultString;
	}

	public void setStgResultString(Map<String, String> stgResultString) {
		this.stgResultString = stgResultString;
	}

	public Map<String, Date> getStgResultDate() {
		return stgResultDate;
	}

	public void setStgResultDate(Map<String, Date> stgResultDate) {
		this.stgResultDate = stgResultDate;
	}
}
