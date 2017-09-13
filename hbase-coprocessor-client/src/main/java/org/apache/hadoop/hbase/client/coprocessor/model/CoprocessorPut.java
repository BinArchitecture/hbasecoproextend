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

import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbasePutException;
import org.apache.hadoop.hbase.client.coprocessor.util.HbaseUtil;
import org.apache.hadoop.hbase.util.Bytes;
@SuppressWarnings("deprecation")
public class CoprocessorPut {
	private String rowkey;
	private String familyName;
	private Map<String,String> rowkValue;
	public CoprocessorPut(String familyName,String rowkey,Map<String,String> rowkValue) {
		this.familyName = familyName;
		this.rowkey = rowkey;
		this.rowkValue = rowkValue;
	}

	public Put buildPut() throws HbasePutException {
		Put put = new Put(Bytes.toBytes(rowkey));
		for (String column : rowkValue.keySet()) {
			put.add(Bytes.toBytes(familyName), Bytes.toBytes(column),
					Bytes.toBytes(rowkValue.get(column)));
		}
		HbaseUtil.checkprePutValid(put);
		return put;
	}
}
