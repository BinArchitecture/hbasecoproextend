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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.AggGroupByHandlerIntegerface;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.AggGroupByHandlerProxy.AggGroupByEnum;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.AggregationGroupByClient;
import org.apache.hadoop.hbase.client.coprocessor.model.AggGroupResult;

public class MinAggGroupByHandler implements AggGroupByHandlerIntegerface{
	private static MinAggGroupByHandler instance=new MinAggGroupByHandler();
	private MinAggGroupByHandler(){
	}
	public static MinAggGroupByHandler getInstance(){
		return instance;
	}
	@Override
	public AggGroupResult groupBy(String tableName,String qulifier,String classType,AggGroupByEnum type,
			List<String> groupby, Scan scan,
			AggregationGroupByClient aggregationGroupByClient,Set<String> scanRange) {
		try {
			AggGroupResult result=new AggGroupResult();
			if(classType.equals(String.class.getSimpleName())){
				Map<String,String> stgResult=aggregationGroupByClient.min(TableName.valueOf(tableName), qulifier,classType, groupby, scan,scanRange);
				result.setStgResultString(stgResult);
			}
			else if(classType.equals(Double.class.getSimpleName())){
				Map<String,Double> stgResult=aggregationGroupByClient.min(TableName.valueOf(tableName), qulifier,classType, groupby, scan,scanRange);
				result.setStgResult(stgResult);
			}
			else if(classType.equals(Date.class.getSimpleName())){
				Map<String,Date> stgResult=aggregationGroupByClient.min(TableName.valueOf(tableName), qulifier,classType, groupby, scan,scanRange);
				result.setStgResultDate(stgResult);
			}
			return result;
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
}
