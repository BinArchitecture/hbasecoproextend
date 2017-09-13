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

import org.apache.hadoop.hbase.client.coprocessor.MultiThreadScan.GroupByScanRunable;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.AggGroupByHandlerProxy.AggGroupByEnum;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.impl.AvgAggGroupByResultHandler;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.impl.CountAggGroupByResultHandler;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.impl.MaxAggGroupByResultHandler;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.impl.MinAggGroupByResultHandler;
import org.apache.hadoop.hbase.client.coprocessor.agggroupby.impl.SumAggGroupByResultHandler;
import org.apache.hadoop.hbase.client.coprocessor.model.AggGroupResult;

public class AggGroupByResultHandlerProxy {
	
	private static AggGroupByResultHandlerProxy instance=new AggGroupByResultHandlerProxy();
	private AggGroupByResultHandlerProxy(){
		aggHandlerMap=new HashMap<AggGroupByEnum,AggGroupByResultHandlerInterface>(5);
		aggHandlerMap.put(AggGroupByEnum.avg,AvgAggGroupByResultHandler.getInstance());
		aggHandlerMap.put(AggGroupByEnum.min,MinAggGroupByResultHandler.getInstance());
		aggHandlerMap.put(AggGroupByEnum.max,MaxAggGroupByResultHandler.getInstance());
		aggHandlerMap.put(AggGroupByEnum.sum,SumAggGroupByResultHandler.getInstance());
		aggHandlerMap.put(AggGroupByEnum.count,CountAggGroupByResultHandler.getInstance());
	}
	public static AggGroupByResultHandlerProxy getInstance(){
		return instance;
	}
	private Map<AggGroupByEnum,AggGroupByResultHandlerInterface> aggHandlerMap;
	public void aggResult(List<GroupByScanRunable> lls, AggGroupByEnum type,AggGroupResult result, String classType)throws IllegalStateException {
		 aggHandlerMap.get(type).aggResult(lls,result,classType);
	}
}
