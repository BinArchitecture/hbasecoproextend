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
package org.apache.hadoop.hbase.client.coprocessor.agg;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.agg.impl.AvgAggHandler;
import org.apache.hadoop.hbase.client.coprocessor.agg.impl.CountAggHandler;
import org.apache.hadoop.hbase.client.coprocessor.agg.impl.MaxAggHandler;
import org.apache.hadoop.hbase.client.coprocessor.agg.impl.MinAggHandler;
import org.apache.hadoop.hbase.client.coprocessor.agg.impl.SumAggHandler;
import org.apache.hadoop.hbase.client.coprocessor.model.AggResult;

public class AggHandlerProxy implements AggHandlerIntegerface{
	private static AggHandlerProxy instance=new AggHandlerProxy();
	private AggHandlerProxy(){
		aggHandlerMap=new HashMap<AggEnum,AggHandlerIntegerface>(5);
		aggHandlerMap.put(AggEnum.avg,AvgAggHandler.getInstance());
		aggHandlerMap.put(AggEnum.min,MinAggHandler.getInstance());
		aggHandlerMap.put(AggEnum.max,MaxAggHandler.getInstance());
		aggHandlerMap.put(AggEnum.sum,SumAggHandler.getInstance());
		aggHandlerMap.put(AggEnum.count,CountAggHandler.getInstance());
	}
	public static AggHandlerProxy getInstance(){
		return instance;
	}
	private Map<AggEnum,AggHandlerIntegerface> aggHandlerMap;
	
	
	public enum AggEnum {
		avg,max,min,count,sum
	}


	@Override
	public AggResult agg(String tableName, String qulifier, String classType,
			AggEnum type, Scan scan, AggregationClient aggregationClient,
			Set<String> scanRange) {
		return aggHandlerMap.get(type).agg(tableName, qulifier, classType, type, scan, aggregationClient, scanRange);
	}

}
