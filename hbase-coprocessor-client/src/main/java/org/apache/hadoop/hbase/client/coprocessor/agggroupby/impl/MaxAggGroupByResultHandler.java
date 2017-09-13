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
package org.apache.hadoop.hbase.client.coprocessor.agggroupby.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.client.coprocessor.MultiThreadScan.GroupByScanRunable;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.AggGroupByResultHandlerInterface;
import org.apache.hadoop.hbase.client.coprocessor.model.AggGroupResult;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;

public class MaxAggGroupByResultHandler implements AggGroupByResultHandlerInterface{
	
	private static AggGroupByResultHandlerInterface instance=new MaxAggGroupByResultHandler();

	public static AggGroupByResultHandlerInterface getInstance(){
		return instance;
	}
	
	@Override
	public void aggResult(List<GroupByScanRunable> lls, AggGroupResult aggResult,
			String classType) throws IllegalStateException {
		if(classType.equals(String.class.getSimpleName())){
			List<Map<String,String>> lla=new ArrayList<Map<String,String>>(lls.size());
			for(GroupByScanRunable srn:lls){
				AggGroupResult agg=srn.getResult();
				if(agg==null)
					throw new IllegalStateException("stg can not be null");
				lla.add(agg.getStgResultString());
			}
			Map<String,String> stgResult=HbaseUtil.calcMax(lla,String.class);
			aggResult.setStgResultString(stgResult);
		}
		else if(classType.equals(Double.class.getSimpleName())){
			List<Map<String,Double>> lla=new ArrayList<Map<String,Double>>(lls.size());
			for(GroupByScanRunable srn:lls){
				AggGroupResult agg=srn.getResult();
				if(agg==null)
					throw new IllegalStateException("stg can not be null");
				lla.add(agg.getStgResult());
			}
			Map<String,Double> stgResult=HbaseUtil.calcMax(lla,Double.class);
			aggResult.setStgResult(stgResult);
		}
		else if(classType.equals(Date.class.getSimpleName())){
			List<Map<String,Date>> lla=new ArrayList<Map<String,Date>>(lls.size());
			for(GroupByScanRunable srn:lls){
				AggGroupResult agg=srn.getResult();
				if(agg==null)
					throw new IllegalStateException("stg can not be null");
				lla.add(agg.getStgResultDate());
			}
			Map<String,Date> stgResult=HbaseUtil.calcMax(lla,Date.class);
			aggResult.setStgResultDate(stgResult);
		}
	}
}