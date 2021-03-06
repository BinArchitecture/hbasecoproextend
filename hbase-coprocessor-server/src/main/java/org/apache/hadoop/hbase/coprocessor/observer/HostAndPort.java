package org.apache.hadoop.hbase.coprocessor.observer;
public class HostAndPort{
		private String host;
		private String name;
		private int port;
		
		public HostAndPort() {
		}
		
		public HostAndPort(String host, String name, int port) {
			this.host = host;
			this.name = name;
			this.port = port;
		}
		
		public HostAndPort(String hostAndNameAndPort) {
			String [] tmp = hostAndNameAndPort.split(":");
			if (tmp.length == 3) {
				this.host = tmp[0];
				this.name = tmp[1];
				this.port = Integer.valueOf(tmp[2]);
			}
		}
		
		public String getHost() {
			return host;
		}
		public void setHost(String host) {
			this.host = host;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public int getPort() {
			return port;
		}
		public void setPort(int port) {
			this.port = port;
		}
		
		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("HostAndPort [host=");
			builder.append(host);
			builder.append(", name=");
			builder.append(name);
			builder.append(", port=");
			builder.append(port);
			builder.append("]");
			return builder.toString();
		}
	}