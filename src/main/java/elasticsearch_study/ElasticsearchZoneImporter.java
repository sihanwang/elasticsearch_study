package elasticsearch_study;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.InetAddress;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/*
PUT /vessel_zone
{
  "mappings": {
    "zone": {
      "properties": {
        "id": {
          "type": "integer"
        },
        "name": {
          "type": "string"
        },
        "type": {
          "type": "String",
          "index": "not_analyzed"
        },
        "boundary": {
          "type": "geo_shape"
        }
      }
    } 
  }
}
*/
public class ElasticsearchZoneImporter {

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

		BufferedReader br = new BufferedReader(new FileReader(
				"D:/vesselzone_new.txt"));
		String data = br.readLine();// 一次读入一行，直到读入null为文件结束
		int i=1;
		while (data != null) {
			System.out.println(data);

			IndexResponse response = client.prepareIndex("vessel_zone", "zone",String.valueOf(i))
					.setSource(data).get();

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
			data = br.readLine(); // 接着读下一行
			i++;
		}
		br.close();

	}

}
