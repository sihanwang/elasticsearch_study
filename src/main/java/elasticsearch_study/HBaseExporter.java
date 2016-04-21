package elasticsearch_study;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;


import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class HBaseExporter {
	
	public static byte[] TYPE = Bytes.toBytes("TYPE");
	public static byte[] ves = Bytes.toBytes("ves");
	public static byte[] NAME = Bytes.toBytes("NAME");

	public static void main(String[] args) throws Exception {

		PropertyConfigurator.configure("log4j.properties");
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path("hbase-site.xml"));
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("cdb_vessel",
				"latest_location"));

		TableName Vessel_Name = TableName
				.valueOf("cdb_vessel:vessel");
		Table Vessel_Table = connection.getTable(Vessel_Name);
		
		BufferedWriter writer  = new BufferedWriter(new FileWriter("D:/vessel_latestlocation.txt"));  
		
		
		Scan scan1 = new Scan(); // co ScanExample-1-NewScan Create empty Scan
									// instance.
		scan1.setCaching(1000);
		ResultScanner scanner1 = table.getScanner(scan1);
		

		for (Result res : scanner1) {
			System.out.println(res); // co ScanExample-3-Dump Print row content.
			String IMO = "";
			String timestamp = "";
			double lon=0;
			double lat=0;

			for (Cell cell : res.rawCells()) {

				String rawkey = Bytes.toString(CellUtil.cloneRow(cell));
				String Qualifier = Bytes
						.toString(CellUtil.cloneQualifier(cell));
				String Value = Bytes.toString(CellUtil.cloneValue(cell));

				lon = Double.valueOf(rawkey.substring(4, 15)) * 0.0000001;

				lat = Double.valueOf(rawkey.substring(15)) * 0.0000001;

				if (Qualifier.equals("imo")) {
					IMO = Value;
				} else if (Qualifier.equals("timestamp")) {
					timestamp = Value;
				}

			}
			
			String[] name_type=getVesselName_Type(Vessel_Table,LpadNum(IMO, 7)) ;

			String source = "{ \"imo\":\"" + IMO+ "\","
			+" \"name\":\""+name_type[0]+"\","
			+" \"type\":\""+name_type[1]+"\","					
			+" \"location\":{\"type\":\"point\",\"coordinates\":["
					+ String.valueOf(lon) + "," + String.valueOf(lat)
					+ "]},\"record_time\":\"" + timestamp + "\" }"+System.getProperty("line.separator");
			
			writer.write(source);
			writer.flush();

		}
		scanner1.close(); 
		writer.close();


	}
	
	private static String[] getVesselName_Type(Table Vessel_Table,String IMO_str) throws IOException
	{
		 Get get = new Get(Bytes.toBytes(IMO_str));
		 get.addColumn(ves, TYPE);
		 get.addColumn(ves, NAME);	

		 
		 Result result = Vessel_Table.get(get);
		 byte[] type = result.getValue(ves,TYPE);
		 byte[] name = result.getValue(ves,NAME);
		 

		return new String[]{Bytes.toString(name),Bytes.toString(type)};
	}
	
	private static String LpadNum(String res, int pad) {
		if (pad > 0) {
			while (res.length() < pad) {
				res = "0" + res;
			}
		}
		return res;
	}

}
