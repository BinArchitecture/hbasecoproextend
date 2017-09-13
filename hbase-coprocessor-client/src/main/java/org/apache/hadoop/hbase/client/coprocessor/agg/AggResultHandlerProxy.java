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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.coprocessor.MultiThreadScan.ScanAggRunable;
import org.apache.hadoop.hbase.client.coprocessor.agg.AggHandlerProxy.AggEnum;
import org.apache.hadoop.hbase.client.coprocessor.agg.impl.AvgAggResultHandler;
import org.apache.hadoop.hbase.client.coprocessor.agg.impl.CountAggResultHandler;
import org.apache.hadoop.hbase.client.coprocessor.agg.impl.MaxAggResultHandler;
import org.apache.hadoop.hbase.client.coprocessor.agg.impl.MinAggResultHandler;
import org.apache.hadoop.hbase.client.coprocessor.agg.impl.SumAggResultHandler;
import org.apache.hadoop.hbase.client.coprocessor.model.AggResult;

public class AggResultHandlerProxy {
	
	private static AggResultHandlerProxy instance=new AggResultHandlerProxy();
	private AggResultHandlerProxy(){
		aggHandlerMap=new HashMap<AggEnum,AggResultHandlerInterface>(5);
		aggHandlerMap.put(AggEnum.avg,AvgAggResultHandler.getInstance());
		aggHandlerMap.put(AggEnum.min,MinAggResultHandler.getInstance());
		aggHandlerMap.put(AggEnum.max,MaxAggResultHandler.getInstance());
		aggHandlerMap.put(AggEnum.sum,SumAggResultHandler.getInstance());
		aggHandlerMap.put(AggEnum.count,CountAggResultHandler.getInstance());
	}
	public static AggResultHandlerProxy getInstance(){
		return instance;
	}
	private Map<AggEnum,AggResultHandlerInterface> aggHandlerMap;

	public void aggResult(List<ScanAggRunable> lls, AggEnum type,AggResult result, String classType)throws IllegalStateException {
		 aggHandlerMap.get(type).aggResult(lls,result,classType);
	}

}
