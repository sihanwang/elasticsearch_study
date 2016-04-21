package elasticsearch_study;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.InetAddress;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
PUT /vessel_latest_location
{
  "mappings": {
    "location": {
      "properties": {
        "imo": {
          "type": "integer"
        },
        "name": {
          "type": "string"
        },
        "record_time": {
          "type": "date",
          "format": "yyyy-MM-dd'T'HH:mm:ss"
        },
        "type": {
          "type": "String",
          "index": "not_analyzed"
        },
        "location": {
          "type": "geo_shape"
        }
      }
    }
  }
}
*/


public class ElasticsearchLocationImporter {

	public static void main(String[] args) throws Exception {

		// TODO Auto-generated method stub
		// ////////////////////////////////////////////////////////////////////////////////////
		Settings settings = Settings.settingsBuilder()
				.put("cluster.name", "beijinges").build();
		Client client = TransportClient
				.builder()
				.settings(settings)
				.build()
				.addTransportAddress(
						new InetSocketTransportAddress(InetAddress
								.getByName("es.bigdata.bjoc.int.thomsonreuters.com"), 9300));
		// /////////////////////////////////////////////////////////////////////////////////////////
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(1);

		BufferedReader br = new BufferedReader(new FileReader(
				"D:/vessel_latestlocation.txt"));
		String data = br.readLine();// 一次读入一行，直到读入null为文件结束
		while (data != null) {
			ESLocationtask es = new ESLocationtask(data, client);
			fixedThreadPool.execute(es);
			data = br.readLine(); // 接着读下一行
		}
		br.close();

	}

}

class ESLocationtask implements Runnable {

	private String Data;
	private Client client;

	public ESLocationtask(String data, Client cl) {
		Data = data;
		client = cl;
	}

	public void run() {
		try {
			System.out.println(Data);

			IndexResponse response = client.prepareIndex("vessel_latest_location", "location")
					.setSource(Data).get();

			// Index name
			String _index = response.getIndex();
			// Type name
			String _type = response.getType();
			// Document ID (generated or not)
			String _id = response.getId();
			// Version (if it's the first time you index this document, you will
			// get: 1)
			long _version = response.getVersion();
			// isCreated() is true if the document is a new one, false if it has
			// been updated
			boolean created = response.isCreated();

			System.out.println(_index + "/" + _type + "/" + _id + "/"
					+ _version + "/" + created);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}