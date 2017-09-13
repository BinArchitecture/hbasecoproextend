package org.apache.hadoop.hbase.client.coprocessor.model.ddl;

import java.io.Serializable;

import org.apache.hadoop.hbase.client.coprocessor.model.FamilyCond;
import org.apache.hadoop.hbase.client.coprocessor.model.HbaseOp;

public class HBaseModifiedParam implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 9071610179793259544L;
	private String tableName;
	private HbaseOp op;
	private FamilyCond fc;
	private String coproceeClassName;
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public HbaseOp getOp() {
		return op;
	}
	public void setOp(HbaseOp op) {
		this.op = op;
	}
	public FamilyCond getFc() {
		return fc;
	}
	public void setFc(FamilyCond fc) {
		this.fc = fc;
	}
	public String getCoproceeClassName() {
		return coproceeClassName;
	}
	public void setCoproceeClassName(String coproceeClassName) {
		this.coproceeClassName = coproceeClassName;
	}
	
	public HBaseModifiedParam build(String tableName,HbaseOp op,FamilyCond fc,String coproceeClassName){
		this.tableName=tableName;
		this.op=op;
		this.fc=fc;
		this.coproceeClassName=coproceeClassName;
		return this;
	}
}