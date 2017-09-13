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
package org.apache.hadoop.hbase.client.coprocessor.agg.impl;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.agg.AggHandlerIntegerface;
import org.apache.hadoop.hbase.client.coprocessor.agg.AggHandlerProxy.AggEnum;
import org.apache.hadoop.hbase.client.coprocessor.agg.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.model.AggResult;

public class MaxAggHandler implements AggHandlerIntegerface {
	private static MaxAggHandler instance = new MaxAggHandler();

	private MaxAggHandler() {
	}

	public static MaxAggHandler getInstance() {
		return instance;
	}

	@Override
	public AggResult agg(String tableName, String qulifier, String classType, AggEnum type, Scan scan,
			AggregationClient aggregationClient, Set<String> scanRange) {
		AggResult result = new AggResult();
		try {
			if (classType.equals(String.class.getSimpleName())) {
				String agg = aggregationClient.max(TableName.valueOf(tableName), qulifier, classType, scan,
						scanRange);
				result.setStgResultString(agg);

			} else if (classType.equals(Double.class.getSimpleName())) {
				BigDecimal agg = aggregationClient.max(TableName.valueOf(tableName), qulifier, classType, scan,
						scanRange);
				result.setStgResult(agg);
			} else if (classType.equals(Date.class.getSimpleName())) {
				Date agg = aggregationClient.max(TableName.valueOf(tableName), qulifier, classType, scan,
						scanRange);
				result.setStgResultDate(agg);
			}
			return result;
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}

}
