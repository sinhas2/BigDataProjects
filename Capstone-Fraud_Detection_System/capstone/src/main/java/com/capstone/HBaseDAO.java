package com.capstone;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

public class HBaseDAO {

	/*
	 * Get instance of HBase table name
	 */
	public static Table GetHBaseTable(String HBaseServerIP, String tableName) throws IOException {

		Table table = null;
		Connection hbaseConnection = HbaseConnection.GetHBaseConnection(HBaseServerIP);
		table = hbaseConnection.getTable(TableName.valueOf(tableName));

		return table;
	}

	public static Result getLookupDataRow(String cardId, Table table) throws IOException {

		Get g = new Get(Bytes.toBytes(cardId));
		Result resultRow = table.get(g);
		return resultRow;
	}

}
