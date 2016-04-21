package elasticsearch_study;

import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Map;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.WKTReader;

import net.sf.json.JSONObject;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;


public class DistanceCalculator {

	public static void testWriteReadNoProperties() throws Exception {
		SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
		tb.add("geom", Point.class, DefaultGeographicCRS.WGS84);
		tb.add("name", String.class);
		tb.add("quantity", Integer.class);
		tb.setName("outbreak");
		SimpleFeatureType schema = tb.buildFeatureType();

		SimpleFeatureBuilder fb = new SimpleFeatureBuilder(schema);
		fb.add(new WKTReader().read("POINT(10 20)"));
		SimpleFeature feature = fb.buildFeature("outbreak.1");

		FeatureJSON fj = new FeatureJSON();
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		fj.writeFeature(feature, os);

		String json = os.toString();

		// here it would break because the written json was incorrect
		SimpleFeature feature2 = fj.readFeature(json);
		//assertEquals(feature.getID(), feature2.getID());
	}

	public static Geometry deserialize() throws Exception{
		GeometryJSON json = new GeometryJSON(); 
		ByteArrayInputStream stream = new ByteArrayInputStream("{\"type\":\"point\",\"coordinates\":[-175.82254999999998,-74.8195317]}".getBytes()); 
		return json.read(stream); 
	} 

	public static void main(String[] args) throws Exception {

		Geometry a = deserialize();
		// TODO Auto-generated method stub

		/*
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
		Coordinate coord = new Coordinate(Double.parseDouble(longlat[1]), Double.parseDouble(longlat[0]));
		Point point = geometryFactory.createPoint(coord);
		 */

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

		SearchResponse responsealllocations = client.prepareSearch("point")
				.setTypes("vessel")
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setSize(10000)
				.setQuery(qballlocations)
				.execute().actionGet();

		for (SearchHit hitalllocations : responsealllocations.getHits().getHits()) {


			JSONObject jsonDoc=JSONObject.fromObject(hitalllocations.getSource());


			String IMO=(String)jsonDoc.get("imo");
			String geojson_point=jsonDoc.get("location").toString();

			GeometryJSON g = new GeometryJSON();
			Point thispoint=g.readPoint(new ByteArrayInputStream(geojson_point.getBytes()));


			for (int i=1 ; i<=8073; i++)
			{
				GetResponse getresponse = client.prepareGet("shape", "vessel_polygon", String.valueOf(i))
						.setOperationThreaded(false)
						.get();			

				if (getresponse.isExists()) {
					Map<String, Object> json = getresponse.getSourceAsMap();
					String polygon_name = (String) json.get("name");
					String geojson_polygon=(String) json.get("boundary");

					Geometry geometry=g.read(new ByteArrayInputStream(geojson_polygon.getBytes()));

					System.out.println(thispoint.distance(geometry));
				}


			}

		}		

		client.close();


	}

}
