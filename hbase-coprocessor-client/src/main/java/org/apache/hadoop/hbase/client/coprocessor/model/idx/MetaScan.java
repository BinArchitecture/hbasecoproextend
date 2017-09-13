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
package org.apache.hadoop.hbase.client.coprocessor.model.idx;

import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.ScanOrderedKV;

public class MetaScan {
	private Scan scan;
	private MetaIndex mi;
	private List<TreeMap<String, String>> complexQulifierCond;
	private List<ScanOrderedKV> simpleQulifierCond;

	public Scan getScan() {
		return scan;
	}

	public void setScan(Scan scan) {
		this.scan = scan;
	}

	public MetaIndex getMi() {
		return mi;
	}

	public void setMi(MetaIndex mi) {
		this.mi = mi;
	}

	public MetaScan(Scan scan, MetaIndex mi) {
		this.mi = mi;
		this.scan = scan;
	}

	public List<TreeMap<String, String>> getComplexQulifierCond() {
		return complexQulifierCond;
	}

	public void setComplexQulifierCond(
			List<TreeMap<String, String>> complexQulifierCond) {
		this.complexQulifierCond = complexQulifierCond;
	}

	public List<ScanOrderedKV> getSimpleQulifierCond() {
		return simpleQulifierCond;
	}

	public void setSimpleQulifierCond(List<ScanOrderedKV> simpleQulifierCond) {
		this.simpleQulifierCond = simpleQulifierCond;
	}
}