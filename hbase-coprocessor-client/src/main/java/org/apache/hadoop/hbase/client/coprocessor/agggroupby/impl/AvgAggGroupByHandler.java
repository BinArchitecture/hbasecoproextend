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

import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.AggGroupByHandlerIntegerface;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.AggGroupByHandlerProxy.AggGroupByEnum;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.AggregationGroupByClient;
import org.apache.hadoop.hbase.client.coprocessor.model.AggGroupResult;
import org.apache.hadoop.hbase.client.coprocessor.model.groupby.Avg;

public class AvgAggGroupByHandler implements AggGroupByHandlerIntegerface{
	private static AvgAggGroupByHandler instance=new AvgAggGroupByHandler();
	private AvgAggGroupByHandler(){
	}
	public static AvgAggGroupByHandler getInstance(){
		return instance;
	}
	@Override
	public AggGroupResult groupBy(String tableName,String qulifier,String classType, AggGroupByEnum type,
			List<String> groupby, Scan scan,
			AggregationGroupByClient aggregationGroupByClient,Set<String> scanRange) {
		try {
			List<Avg> llavg=aggregationGroupByClient.avg(TableName.valueOf(tableName), qulifier, groupby, scan,scanRange);
			AggGroupResult result=new AggGroupResult();
			result.setListAvg(llavg);
			return result;
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}
}