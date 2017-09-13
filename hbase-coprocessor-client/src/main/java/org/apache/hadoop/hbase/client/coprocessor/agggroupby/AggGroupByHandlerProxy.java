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
package org.apache.hadoop.hbase.client.coprocessor.agggroupby;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.impl.AvgAggGroupByHandler;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.impl.CountAggGroupByHandler;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.impl.MaxAggGroupByHandler;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.impl.MinAggGroupByHandler;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.impl.SumAggGroupByHandler;
import org.apache.hadoop.hbase.client.coprocessor.model.AggGroupResult;

public class AggGroupByHandlerProxy implements AggGroupByHandlerIntegerface{
	private static AggGroupByHandlerProxy instance=new AggGroupByHandlerProxy();
	private AggGroupByHandlerProxy(){
		aggHandlerMap=new HashMap<AggGroupByEnum,AggGroupByHandlerIntegerface>(5);
		aggHandlerMap.put(AggGroupByEnum.avg,AvgAggGroupByHandler.getInstance());
		aggHandlerMap.put(AggGroupByEnum.min,MinAggGroupByHandler.getInstance());
		aggHandlerMap.put(AggGroupByEnum.max,MaxAggGroupByHandler.getInstance());
		aggHandlerMap.put(AggGroupByEnum.sum,SumAggGroupByHandler.getInstance());
		aggHandlerMap.put(AggGroupByEnum.count,CountAggGroupByHandler.getInstance());
	}
	public static AggGroupByHandlerProxy getInstance(){
		return instance;
	}
	private Map<AggGroupByEnum,AggGroupByHandlerIntegerface> aggHandlerMap;
	
	
	public enum AggGroupByEnum {
		avg,max,min,count,sum
	}

	@Override
	public AggGroupResult groupBy(String tableName,String qulifier,String classType,AggGroupByEnum type,
			List<String> groupby, Scan scan,
			AggregationGroupByClient aggregationGroupByClient,Set<String> scanRange) {
		return aggHandlerMap.get(type).groupBy(tableName,qulifier,classType,type, groupby, scan, aggregationGroupByClient, scanRange);
	}
}
