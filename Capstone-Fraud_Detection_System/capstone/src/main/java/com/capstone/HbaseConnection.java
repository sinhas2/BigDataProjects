package com.capstone;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;


public class HbaseConnection implements Serializable {

	private static final long serialVersionUID = 1L;
	static Connection hbaseConnection = null;

	static Connection GetHBaseConnection(String HBaseServerIP) throws IOException {

		try {

			if (hbaseConnection == null) {

				org.apache.hadoop.conf.Configuration conf = (org.apache.hadoop.conf.Configuration) HBaseConfiguration
						.create();
				conf.setInt("timeout", 1200);
				conf.set("hbase.master", HBaseServerIP + ":60000");
				conf.set("hbase.zookeeper.quorum", HBaseServerIP);
				conf.set("hbase.zookeeper.property.clientPort", "2181");
				conf.set("zookeeper.znode.parent", "/hbase");

				hbaseConnection = ConnectionFactory.createConnection(conf);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return hbaseConnection;

	}

}
