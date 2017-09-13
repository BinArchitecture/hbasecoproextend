package org.apache.hadoop.hbase.client.coprocessor.model.ddl;

import java.io.Serializable;

import org.apache.hadoop.hbase.client.coprocessor.model.FamilyCond;

public class HBaseCrTableParam implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 4225930787731173198L;
	private String tableName;
	private FamilyCond[] familyCrArray;
	private int regionNum;
	private int es2ndTotal=1;
	private String mainFamilyName;
	private boolean isCompress;
	public HBaseCrTableParam build(String tableName,FamilyCond[] familyCrArray,int regionNum,String mainFamilyName,boolean isCompress, int... es2total){
		this.tableName=tableName;
		this.familyCrArray=familyCrArray;
		this.regionNum=regionNum;
		this.mainFamilyName=mainFamilyName;
		this.isCompress=isCompress;
		if(es2total!=null&&es2total.length>0)
			this.es2ndTotal=es2total[0];
		return this;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public int getRegionNum() {
		return regionNum;
	}
	public void setRegionNum(int regionNum) {
		this.regionNum = regionNum;
	}
	public String getMainFamilyName() {
		return mainFamilyName;
	}
	public void setMainFamilyName(String mainFamilyName) {
		this.mainFamilyName = mainFamilyName;
	}
	public boolean isCompress() {
		return isCompress;
	}
	public void setCompress(boolean isCompress) {
		this.isCompress = isCompress;
	}
	public FamilyCond[] getFamilyCrArray() {
		return familyCrArray;
	}
	public void setFamilyCrArray(FamilyCond[] familyCrArray) {
		this.familyCrArray = familyCrArray;
	}
	public int getEs2ndTotal() {
		return es2ndTotal;
	}
	public void setEs2ndTotal(int es2ndTotal) {
		this.es2ndTotal = es2ndTotal;
	}
}