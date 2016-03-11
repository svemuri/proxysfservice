package hello;

import hello.CatalogResult.TableColumnDescriptor;
import hello.CatalogResult.TableDescriptor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.core.MultivaluedMap;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.util.MultiValueMap;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.Base64;
import com.sun.jersey.core.util.MultivaluedMapImpl;


public class SFExecutor {
	private static Map<String,String> credentialsMap = new HashMap<String,String>();
	
	

	private String authToken="NULL";
	private String authURL = 
			"https://login.salesforce.com/services/oauth2/token";
	private String baseURL = "https://na22.salesforce.com/services/data/v20.0";
	private String queryURL = baseURL+"/query";
	private String catalogURL = baseURL+"/sobjects";

	private QueryResult queryResult = null;
	
	private void printHttpHeaders(MultiValueMap<String, String> headers) {
		// TODO Auto-generated method stub
		for (Entry<String, List<String>> entry : headers.entrySet()) {
			  System.out.println("http header key = " + entry.getKey());
			  System.out.print(" value = " );
			  for (String v: entry.getValue())
				  System.out.print(" " + v);
			  System.out.println("");
		}
	}

	public SFExecutor(MultiValueMap<String, String> headers) {
		printHttpHeaders(headers);

		setCredentials(headers);
		System.setProperty("https.proxyHost", "www-proxy.us.oracle.com");
		System.setProperty("https.proxyPort", "80");
		System.setProperty("http.proxyHost", "www-proxy.us.oracle.com");
		System.setProperty("http.proxyPort", "80");
		refreshAuthToken();

	}

	private void setDefaultCredentials(){
		
		System.out.println("WARNING: using default credentials");
		 credentialsMap.put("client_id",
				 "3MVG9uudbyLbNPZMW7oSwnN.yHZHV3UUL2zKsAMRvocGN4GGNpT7lpht4qiU3E531bQZXJQ_ba5_eAasLXFA3");
		 credentialsMap.put("username",  "johny.cash3456@gmail.com");
		 credentialsMap.put("password", "salesforce14" + "5zSSQFPdvBmUHaZhspXEUoNCL");
				
		
		
		 credentialsMap.put("client_secret","5939251840186055793");
	}
	
	private void setCredentials(MultiValueMap<String, String> headers) {
		// TODO Auto-generated method stub
		if (headers == null || !headers.containsKey("extracredentials")){
			setDefaultCredentials();
			return;
		}
		
		String credentialsString = headers.getFirst("extracredentials");
		String[] keyValues = credentialsString.split(",");
		for (String kv: keyValues){
			String[] s = kv.split(":");
			credentialsMap.put(s[0].trim(), s[1].trim());
		}
		
		// update password
		credentialsMap.put("password", credentialsMap.get("password")+credentialsMap.get("secret_token"));
		
		
	}

	public QueryResult executeQuery(String query) {
		// TODO Auto-generated method stub
		MultivaluedMap<String, String> params = new MultivaluedMapImpl();

		params.add("q", query);

		System.out.println("Query url = " + queryURL);
		System.out.println("Received query = " + query);
		String resultStr = restCall(queryURL, params);
		System.out.println("Query output = " + resultStr);
		return new QueryResult(resultStr);
	}

	private String restCall(String endPoint, MultivaluedMap<String, String> params) {
		Client client = Client.create();


		WebResource webResource = client.resource(endPoint);

		if (params != null)
			webResource = webResource.queryParams(params);
		ClientResponse response =  webResource
				.accept("application/json")
				.header("Authorization", "OAuth "+authToken)
				.get(ClientResponse.class);

		
		if (response.getStatus() != 200) {
			System.out.println("error response = " + response.getEntity(String.class));
			throw new RuntimeException("Failed trying to execute query: HTTP error code : "
					+ response.getStatus());
		}

		String resultStr = response.getEntity(String.class);
		System.out.println("Query result = " + resultStr);
		return resultStr;
	}

	private void refreshAuthToken() {
		Client client = Client.create();
		MultivaluedMap<String, String> params = new MultivaluedMapImpl();

		params.add("grant_type", "password");
		addCredentialParameters(params);

		WebResource webResource = client.resource(authURL);

		ClientResponse response = webResource
				.accept("application/json")

				.post(ClientResponse.class, params);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed trying to obtain auth token : HTTP error code : "
					+ response.getStatus());
		}

		String output = response.getEntity(String.class);
		System.out.println("authentication output = " + output);
		try {
			JsonNode n = new ObjectMapper().readValue(output, JsonNode.class);
			authToken = n.get("access_token").asText();
			baseURL = n.get("instance_url").asText() + "/services/data/v20.0";
			queryURL = baseURL + "/query";
			catalogURL = baseURL + "/sobjects";
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}

	private void addCredentialParameters(MultivaluedMap<String, String> params) {
		for (String k: credentialsMap.keySet())
			params.add(k,  credentialsMap.get(k));
		
	}

	public QueryResult getQueryResult() {
		// TODO Auto-generated method stub
		return queryResult;
	}

	public List<TableDescriptor> getTables(String schema) {
		// TODO Auto-generated method stub
		String sobjectsJson = restCall(catalogURL, null);
		List<TableDescriptor> result = CatalogResult.getTables(sobjectsJson);
		return result;

	}

	public List<TableColumnDescriptor> getTableColumns(String tableName) {
		String descJson = restCall(catalogURL+"/"+tableName+"/describe", null);
		return CatalogResult.getTableColumns(descJson);
	}


	/*
public static void main(String[] args){
	String q = "select name from ACCOUNT";
	QueryResult r = new SFExecutor(q).getQueryResult();
}
	 */

}

