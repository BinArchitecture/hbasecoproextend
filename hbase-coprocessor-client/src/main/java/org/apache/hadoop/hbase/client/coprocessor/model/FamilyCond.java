package org.apache.hadoop.hbase.client.coprocessor.model;

import java.io.Serializable;

import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;

public class FamilyCond implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -19722028479019446L;
	private String familyName;
	private RowKeyComposition rkc;
	
	public FamilyCond() {
	}

	public FamilyCond(String familyName,RowKeyComposition rkc) {
		this.familyName = familyName;
		this.rkc = rkc;
	}

	public String getFamilyName() {
		return familyName;
	}

	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}

	public RowKeyComposition getRkc() {
		return rkc;
	}

	public void setRkc(RowKeyComposition rkc) {
		this.rkc = rkc;
	}

}