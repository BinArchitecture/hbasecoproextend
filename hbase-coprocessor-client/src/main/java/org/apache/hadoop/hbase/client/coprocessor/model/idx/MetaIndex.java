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

public class MetaIndex implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 477564977148818426L;
	private String idxName;
	private int destPos;
	private int idxPos;
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + destPos;
		result = prime * result + ((idxName == null) ? 0 : idxName.hashCode());
		result = prime * result + idxPos;
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
		MetaIndex other = (MetaIndex) obj;
		if (destPos != other.destPos)
			return false;
		if (idxName == null) {
			if (other.idxName != null)
				return false;
		} else if (!idxName.equals(other.idxName))
			return false;
		if (idxPos != other.idxPos)
			return false;
		return true;
	}

	public int getIdxPos() {
		return idxPos;
	}

	public void setIdxPos(int idxPos) {
		this.idxPos = idxPos;
	}

	public String getIdxName() {
		return idxName;
	}

	public void setIdxName(String idxName) {
		this.idxName = idxName;
	}

	public int getDestPos() {
		return destPos;
	}

	public void setDestPos(int destPos) {
		this.destPos = destPos;
	}
}