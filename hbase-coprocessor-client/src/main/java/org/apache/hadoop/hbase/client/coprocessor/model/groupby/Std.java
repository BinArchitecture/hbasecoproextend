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
package org.apache.hadoop.hbase.client.coprocessor.model.groupby;

import java.io.Serializable;
import java.math.BigDecimal;

public class Std implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6051762369054971870L;
	private BigDecimal sum;
	private int total;
	public Std(){}
	public Std(BigDecimal sum, int i) {
		this.sum = sum;
		this.total = i;
	}

	public BigDecimal getSum() {
		return sum;
	}

	public void setSum(BigDecimal sum) {
		this.sum = sum;
	}

	public int getTotal() {
		return total;
	}

	public void setTotal(int total) {
		this.total = total;
	}

	public Std add(Std st) {
		return st == null ? this : new Std(this.sum.add(st.sum), this.total
				+ st.total);
	}

	public Double buildAvgNumber() {
		return this.sum.divide(BigDecimal.valueOf(total))
				.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
	}
}
