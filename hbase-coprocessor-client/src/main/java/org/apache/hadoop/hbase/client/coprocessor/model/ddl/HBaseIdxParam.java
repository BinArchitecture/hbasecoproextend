package org.apache.hadoop.hbase.client.coprocessor.model.ddl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.coprocessor.model.idx.HbaseDataType;

public class HBaseIdxParam implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7071610179793259544L;
	private String tableName;
	private String idxName; 
	private String familyName;
	private String column;
	private List<String> colnameList;
	private ArrayList<HbaseDataType> orderByList;
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getIdxName() {
		return idxName;
	}
	public void setIdxName(String idxName) {
		this.idxName = idxName;
	}
	public String getFamilyName() {
		return familyName;
	}
	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}
	public List<String> getColnameList() {
		return colnameList;
	}
	public void setColnameList(List<String> colnameList) {
		this.colnameList = colnameList;
	}
	public ArrayList<HbaseDataType> getOrderByList() {
		return orderByList;
	}
	public void setOrderByList(ArrayList<HbaseDataType> orderByList) {
		this.orderByList = orderByList;
	}
	public String getColumn() {
		return column;
	}
	public void setColumn(String column) {
		this.column = column;
	}
	
	public HBaseIdxParam build(String tableName,String idxName,String familyName,List<String> colnameList,ArrayList<HbaseDataType> orderByList){
		this.tableName=tableName;
		this.idxName=idxName;
		this.familyName=familyName;
		this.colnameList=colnameList;
		this.orderByList=orderByList;
		return this;
	}
	
	public HBaseIdxParam build(String tableName,String idxName,String familyName,String column){
		this.tableName=tableName;
		this.idxName=idxName;
		this.familyName=familyName;
		this.column=column;
		return this;
	}
}