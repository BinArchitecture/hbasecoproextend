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

import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.coprocessor.Constants;

public class HbaseDataType implements Serializable {
	/**
	 * 
	 */
	private static final Log LOG = LogFactory.getLog(HbaseDataType.class
			.getName());
	private static final long serialVersionUID = -5096091580493340310L;
	private String qulifier;
	private Class<?> dataType;
	private OrderBy orderBy = OrderBy.ASC;

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((dataType == null) ? 0 : dataType.hashCode());
		result = prime * result + ((orderBy == null) ? 0 : orderBy.hashCode());
		result = prime * result
				+ ((qulifier == null) ? 0 : qulifier.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HbaseDataType other = (HbaseDataType) obj;
		if (dataType == null) {
			if (other.dataType != null)
				return false;
		} else if (dataType!=other.dataType)
			return false;
		if (orderBy != other.orderBy)
			return false;
		if (qulifier == null) {
			if (other.qulifier != null)
				return false;
		} else if (!qulifier.equals(other.qulifier))
			return false;
		return true;
	}

	public String getQulifier() {
		return qulifier;
	}

	public void setQulifier(String qulifier) {
		this.qulifier = qulifier;
	}

	public Class<?> getDataType() {
		return dataType;
	}

	public void setDataType(Class<?> dataType) {
		this.dataType = dataType;
	}

	public OrderBy getOrderBy() {
		return orderBy;
	}

	public void setOrderBy(OrderBy orderBy) {
		this.orderBy = orderBy;
	}

	public String build(String value) {
		if (value == null || orderBy == null)
			return null;
		try {
			if (orderBy.equals(OrderBy.DESC))
				return dataType == Date.class ? parse(Long.toString(
						Constants.HBASEJEDISSEQMAXNUM
								- Constants.getSimpleDateFormat().parse(value)
										.getTime(), 36)) : parse(Long.toString(
						Integer.MAX_VALUE - Integer.parseInt(value), 36));
			else
				return dataType == Date.class ? parse(Long.toString(
						Constants.getSimpleDateFormat().parse(value).getTime(), 36))
						: parse(Long.toString(Integer.parseInt(value), 36));
		} catch (NumberFormatException | ParseException e) {
			LOG.error(e.getMessage(),e);
			return null;
		}
	}

	private String parse(String s) {
		StringBuilder sb = new StringBuilder("");
		if (s.length() <= 8) {
			for (int i = 0; i < 8 - s.length(); i++) {
				sb.append("0");
			}
			sb.append(s);
			return sb.toString();
		}
		return null;
	}
}