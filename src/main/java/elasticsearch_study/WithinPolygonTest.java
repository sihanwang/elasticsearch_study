package elasticsearch_study;

import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

public class WithinPolygonTest {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Settings settings = Settings.settingsBuilder()
				.put("cluster.name", "my-application").build();
		Client client = TransportClient
				.builder()
				.settings(settings)
				.build()
				.addTransportAddress(
						new InetSocketTransportAddress(InetAddress
								.getByName("jing-server-3"), 9300))
								.addTransportAddress(
										new InetSocketTransportAddress(InetAddress
												.getByName("jing-server-5"), 9300));
		
		 PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream("D:/WithinPolygon.txt")),true);   

		for (int i=1 ; i<=8073; i++)
		{
			
			GetResponse getresponse = client.prepareGet("shape", "vessel_polygon", String.valueOf(i))
			        .setOperationThreaded(false)
			        .get();			
			
			if (getresponse.isExists()) {
				Map<String, Object> json = getresponse.getSourceAsMap();
				String polygon_name = (String) json.get("name");

				QueryBuilder qb = geoShapeQuery("location", String.valueOf(i),
						"vessel_polygon", ShapeRelation.WITHIN)
						.indexedShapeIndex("shape")
						.indexedShapePath("boundary");

				// System.out.println(qb.toString());

				SearchResponse searchresponse = client.prepareSearch("point")
						.setTypes("vessel")
						.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
						.setQuery(qb).execute().actionGet();
				
				String result="Query took:"
						+ searchresponse.getTookInMillis() + "milliseconds to find "
						+ searchresponse.getHits().getTotalHits()
						+ " points within "+i+":"+polygon_name;
				pw.println(result);
				System.out.println(result);
				
				/*
			    for (SearchHit hit : response.getHits().getHits()) {
			        //Handle the hit...
			    	Map<String, Object> result = hit.getSource();
			    	System.out.println(result);	    	
			    }
				*/				

			}
		}
		client.close();		
		pw.close();
	}

}
