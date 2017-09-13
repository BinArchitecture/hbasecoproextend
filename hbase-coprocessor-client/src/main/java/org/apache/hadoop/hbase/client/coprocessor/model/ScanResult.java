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

import java.io.Serializable;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.ScanCascadeResult;

public class ScanResult implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -4182622832272757798L;
	private Result r;
	private boolean postNextScan;
	public ScanResult(Result r,boolean postNextScan){
		this.r=r;
		this.postNextScan=postNextScan;
	}
	public ScanResult(){
	}
	public Result getR() {
		return r;
	}
	public void setR(Result r) {
		this.r = r;
	}
	public boolean isPostNextScan() {
		return postNextScan;
	}
	public void setPostNextScan(boolean postNextScan) {
		this.postNextScan = postNextScan;
	}
	public ScanCascadeResult build(){
		ScanCascadeResult scr=null;
		if(postNextScan){
			scr=new CascadeResult(r).build(r);
		}
		else{
			CascadeResult cr=new CascadeResult(r);
			cr.debuild();
			scr=cr.getScr();
		}
		return scr;
	}
}