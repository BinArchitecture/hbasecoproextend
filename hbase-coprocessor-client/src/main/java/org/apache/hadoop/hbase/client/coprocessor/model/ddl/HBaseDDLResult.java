package org.apache.hadoop.hbase.client.coprocessor.model.ddl;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.coprocessor.exception.HbaseDDLException;
import org.apache.hadoop.hbase.client.coprocessor.model.idx.RowKeyComposition;
import org.apache.hadoop.hbase.client.coprocessor.model.scan.StringList;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDDLResult implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4857265656596814916L;
	private String result;
	private List<String> resultIdArray;
	private Map<String,StringList> idxMap;
	private HbaseDDLException excp;
	private RowKeyComposition rkc;
	private Map<String,Map<String,String>> hbaseDescMap;
	public HBaseDDLResult() {
	}

	public HBaseDDLResult(String result, HbaseDDLException excp) {
		this.result = result;
		this.excp = excp;
	}

	public void buildMap(HBaseAdmin admin){
		try {
			HTableDescriptor[] hdc=admin.listTables();
			hbaseDescMap=new HashMap<String,Map<String,String>>();
			for(HTableDescriptor hd:hdc){
				Map<String,String> map=new HashMap<String,String>();
				Map<ImmutableBytesWritable,ImmutableBytesWritable> srcMap=hd.getValues();
				for(ImmutableBytesWritable ibw:srcMap.keySet()){
					ImmutableBytesWritable ibb=srcMap.get(ibw);
					map.put(Bytes.toString(ibw.get()),ibb==null?null:Bytes.toString(ibb.get()));
				}
				hbaseDescMap.put(hd.getNameAsString(), map);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	public HbaseDDLException getExcp() {
		return excp;
	}

	public void setExcp(HbaseDDLException excp) {
		this.excp = excp;
	}

	public Map<String, StringList> getIdxMap() {
		return idxMap;
	}

	public void setIdxMap(Map<String, StringList> idxMap) {
		this.idxMap = idxMap;
	}

	public RowKeyComposition getRkc() {
		return rkc;
	}

	public void setRkc(RowKeyComposition rkc) {
		this.rkc = rkc;
	}

	public Map<String, Map<String, String>> getHbaseDescMap() {
		return hbaseDescMap;
	}

	public void setHbaseDescMap(Map<String, Map<String, String>> hbaseDescMap) {
		this.hbaseDescMap = hbaseDescMap;
	}

	public List<String> getResultIdArray() {
		return resultIdArray;
	}

	public void setResultIdArray(List<String> resultIdArray) {
		this.resultIdArray = resultIdArray;
	}

}