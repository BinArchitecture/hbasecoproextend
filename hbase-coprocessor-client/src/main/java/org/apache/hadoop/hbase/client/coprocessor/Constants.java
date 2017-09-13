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
package org.apache.hadoop.hbase.client.coprocessor;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Constants {
  
  private static final SimpleDateFormat SIMPLEDATEFORMAT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
  public static SimpleDateFormat getSimpleDateFormat(){
	  return new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
  }
  public static Long HBASEJEDISSEQMAXNUM; 
  static{
	  try {
		HBASEJEDISSEQMAXNUM=SIMPLEDATEFORMAT.parse("2105-10-04 00:30:07").getTime();
	} catch (ParseException e) {
		e.printStackTrace();
	}
  }
  public static final String HBASEMAINTABLE = "hbasemaintable";
  public static final String HBASEES2NDTOTAL = "hbasees2ndtotal";
  public static final String HBASEMAINKEY = "hbasemainkey";
  public static final String HBASEPARENTKEY = "hbaseparentkey";
  public static final String HBASEJEDISSEQ = "hbasejedisseq";
  public static final String HBASEJEDISSEQNUM = "hbasejedisseqnum";
  public static final String HBASEHASHMAP = "hbasehashmap";
  public static final String HBASEIDXMODIFYTOPIC = "hbaseidxmodifytopic";
  public static final String HBASEJEDISTOPIC = "hbasejedistopic";
  public static final String HBASEJEDISGROUP = "hbasejedisgroup";
  public static final String HBASEJEDISPREFIX = "$:";
  public static final String HBASEJEDISTABLEMAP = "hbasejedistablemap";
  public static final String HBASEREGIONNUM = "hbaseregionnum";
  public static final String HBASEFAMILY = "hbasefamily";
  public static final String HBASECASCADEMAP = "hbasecascademap";
  public static final String HBASECASCADEROW = "hbasecascaderow";
  public static final String HBASEUPDATEROW = "hbaseupdaterow";
  public static final String HBASEUPDATEROWFORDEL = "hbaseupdaterowfordel";
  public static final String HBASEUPDATEROWFORUPDATE = "hbaseupdaterowforupdate";
  public static final String HBASEERRORUPDATEROW4DEL = "hbaseerrorupdaterow4del";
  public static final String HBASEERRORDELETEROW4DEL = "hbaseerrordeleterow4del";
  public static final String IDXHBASEPUT = "idxhbaseput";
  public static final String IDXHBASEDEL = "idxhbasedel";
  public static final String IDXHBASETABLEDEL = "idxhbasetabledel";
  public static final String REGHBASEPREFTIX = ".*_t_.*(";
  public static final String REGHBASEPREFIIX = ".*_i_";
  public static final String REGDELHBASEPREFIIX = "_i_";
  public static final String REGTABLEHBASEPREFIIX = "_t_";
  public static final String REGTABLEHBASESTART = "_s";
  public static final String REGTABLEHBASESTOP = "_u";
  public static final String REGIDXHBASESTART = "_h";
  public static final String REGIDXHBASESTOP = "_j";
  public static final String SPLITTER = "_";
  public static final String IDXTABLENAMEESPREFIX = "idx_hbase-";
  public static final String REGSPLITTER = "|";
  public static final String QSPLITTER = "::";
  public static final String QSPLITTERORDERBY = ":";
  public static final String QLIFIERSINGLESPLITTER = "#";
  public static final String IDXPREFIX = "0ai";
  public static final String REGSPLITER = ".*";
  public static final String QLIFIERSPLITTER = "##";
  public static final String KAFKASYNCMAP_TOPIC = "hbaseidxsyncMap";
  public static final String KAFKASYNCZK_TOPIC = "hbaseidxsyncZK";
  public static final String KAFKASYNCFLUSH_TOPIC = "hbaseidxsyncFlush";
  public static final String KAFKASYNCESMASTERSYNC_TOPIC = "idxHBaseKafkaEsMasterSync";
  public static final String KAFKASYNCESREGIONSERVERSYNC_TOPIC = "idxHBaseKafkaEsRegionServerSync";
  public static final String KAFKAHBASELOCKED_TOPIC = "kafkahbaselocked";
  public static final String KAFKASYNCZK_GROUP = "hbaseidxsyncZKgroup";
  public static final String KAFKAREGIONSERVERESSYNCZK_GROUP = "hbaseidxRSEsSyncZKgroup";
  public static final String KAFKAMASTERESSYNCZK_GROUP = "hbaseidxmasterEsSyncZKgroup";
  public static final String ZOOINDEXPATH = "/idxHbase";
  public static final String ZOOBINGOADMIN = "/idxHbase/bingoAdmin";
  public static final String ZOOBINGOES = "/idxHbase/bingoEs";
  public static final String ZOOINDEXTBINDEXMAPPATH = "/tbIndex";
  public static final String ZOOINDEXTBINDEXNAMEMAPPATH = "/tbIndexNameMap";
  public static final String HBASETBSPLITCLASSNAME = "org.apache.hadoop.hbase.regionserver.DelimitedKeyPrefixRegionSplitPolicy";
  public static final String DELIMITER_KEY = "DelimitedKeyPrefixRegionSplitPolicy.delimiter";
//  public static final String OIDINFAMILYPOS = "oidInFamilyPos";
  
  public static class Producer
	{
		public static final String metadata_broker_list = "metadata.broker.list";
		public static final String serializer_class = "serializer.class";
		public static final String compression_codec = "compression.codec";
		public static final String compressed_topics = "compressed.topics";
		public static final String message_send_max_retries = "message.send.max.retries";
		public static final String retry_backoff_ms = "retry.backoff.ms";
		public static final String request_required_acks = "request.required.acks";
		public static final String producer_type = "producer.type";
		public static final String batch_num_messages = "batch.num.messages";
		public static final String queue_buffering_max_ms = "queue.buffering.max.ms";
		public static final String queue_enqueue_timeout_ms = "queue.enqueue.timeout.ms";
		public static final String async_thread_num = "async.thread.num";
	}

	public static class Consumer
	{
		public static final String zookeeper_connect = "zookeeper.connect";
		public static final String zookeeper_session_timeout_ms = "zookeeper.session.timeout.ms";
		public static final String zookeeper_connection_timeout_ms = "zookeeper.connection.timeout.ms";
		public static final String rebalance_backoff_ms = "rebalance.backoff.ms";
		public static final String zookeeper_sync_time_ms = "zookeeper.sync.time.ms";
		public static final String auto_commit_interval_ms = "auto.commit.interval.ms";
		public static final String topic_patition_num = "topic.patition.num";
	}
	
	public static class ES
	{
		public static final String clustername = "hbase.es.clustername";
		public static final String clusternodestring = "hbase.es.clusternodestring";
		public static final String maxidle = "hbase.es.maxidle";
		public static final String maxtotal = "hbase.es.maxtotal";
		public static final String maxwaitmillis = "hbase.es.maxwaitmillis";
	}

}
