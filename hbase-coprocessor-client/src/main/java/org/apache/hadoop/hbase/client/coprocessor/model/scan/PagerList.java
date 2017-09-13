package org.apache.hadoop.hbase.client.coprocessor.model.scan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.client.coprocessor.Constants;
import org.apache.hadoop.hbase.client.coprocessor.model.ScanResult;
import org.apache.hadoop.hbase.util.Bytes;

public class PagerList {
	public List<String> getPrefixList() {
		return prefixList;
	}
	public void setPrefixList(List<String> prefixList) {
		this.prefixList = prefixList;
	}

	private int startRow;
	private int limit;
	private List<ScanResult> listResult;
	private List<String> prefixList;
	public int getStartRow() {
		return startRow;
	}
	public void setStartRow(int startRow) {
		this.startRow = startRow;
	}
	public int getLimit() {
		return limit;
	}
	public void setLimit(int limit) {
		this.limit = limit;
	}
	
	public void sort(){
		if(CollectionUtils.isNotEmpty(prefixList)){
			List<ScanResult> tmpList=new ArrayList<ScanResult>(listResult.size());
			Map<String,ScanResult> map=new HashMap<String,ScanResult>(listResult.size());
			if(CollectionUtils.isNotEmpty(listResult)){
				for(ScanResult sr:listResult){
					String prefix=Bytes.toString(sr.build().getSource().getRow()).split(Constants.SPLITTER)[0];
					map.put(prefix, sr);
				}
			}
			for(String s:prefixList){
				ScanResult sr=map.get(s);
				tmpList.add(sr);
			}
			this.setListResult(tmpList);
		}
	}
	
	public List<ScanResult> getListResult() {
		return listResult;
	}
	public void setListResult(List<ScanResult> listResult) {
		this.listResult = listResult;
	}
}
