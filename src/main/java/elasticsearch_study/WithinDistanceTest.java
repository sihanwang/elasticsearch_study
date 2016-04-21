package elasticsearch_study;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import static org.elasticsearch.index.query.QueryBuilders.*;

import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;

public class WithinDistanceTest {

	public static void main(String[] args) throws Exception {
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

		QueryBuilder qballlocations = matchAllQuery();

		SearchResponse responsealllocations = client.prepareSearch("vessel_latest_location")
				.setTypes("location")
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setSize(10000)
				.setQuery(qballlocations)
				.execute().actionGet();

		for (SearchHit hitalllocations : responsealllocations.getHits().getHits()) {

			Map<String, Object> result = hitalllocations.getSource();
			String IMO=(String)result.get("imo");
			Map<String, Object> location=(Map<String, Object>)result.get("location");
			ArrayList<Double> coordinates=(ArrayList<Double>)location.get("coordinates");

			Double lon=coordinates.get(0);
			Double lat=coordinates.get(1);

			QueryBuilder qb = geoShapeQuery(
					"location",                     
					ShapeBuilder.newCircleBuilder().center( lon, lat).radius("10km"),
					ShapeRelation.WITHIN);  		

			//System.out.println(qb.toString());

			SearchResponse response = client.prepareSearch("vessel_latest_location")
					.setTypes("location")				
					.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
					.setQuery(qb)
					.execute().actionGet();

			System.out.println("Query took:"+response.getTookInMillis()+"milliseconds to find "+response.getHits().getTotalHits()+" vessels near IMO:"+IMO);

			/*
			    for (SearchHit hit : response.getHits().getHits()) {
			        //Handle the hit...
			    	Map<String, Object> result = hit.getSource();
			    	System.out.println(result);	    	
			    }
			 */
		}		

		client.close();

	}


}
