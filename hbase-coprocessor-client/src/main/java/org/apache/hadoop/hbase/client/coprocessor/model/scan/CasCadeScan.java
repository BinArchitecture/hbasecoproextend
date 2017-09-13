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
package org.apache.hadoop.hbase.client.coprocessor.model.scan;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ComplexRowFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SubstringComparator;

public class CasCadeScan {
	private Scan scan;
	private Filter oringFilter;
	private Map<String, CasCadeScan> casCadeScanMap;

	public void addPScanKey(String parentKey) {
		if (StringUtils.isBlank(parentKey) || scan == null ||oringFilter ==null)
			return;
		FilterList f = new FilterList(oringFilter);
		f.addFilter(new ComplexRowFilter(CompareFilter.CompareOp.EQUAL,
				new SubstringComparator(parentKey)));
		scan.setFilter(f);
	}

	public Scan getScan() {
		return scan;
	}

	public void setScan(Scan scan) {
		this.scan = scan;
		this.oringFilter=scan.getFilter();
	}

	public Map<String, CasCadeScan> getCasCadeScanMap() {
		return casCadeScanMap;
	}

	public void setCasCadeScanMap(Map<String, CasCadeScan> casCadeScanMap) {
		this.casCadeScanMap = casCadeScanMap;
	}
}
