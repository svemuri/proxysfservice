package hello;

import hello.CatalogResult.TableColumnDescriptor;
import hello.CatalogResult.TableDescriptor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.Base64;
import com.sun.jersey.core.util.MultivaluedMapImpl;


public class SFExecutor {
private static final String clientID =
"3MVG91ftikjGaMd.RnY3d6jjL5vgNZyHgN3c9IvRaIKhnf5Du1cL531_3qj81UYodRdTA9KwtoIGXACmZ_IQP";
private static final String userName = "srinivas.s.vemuri@gmail.com";
private static final String password = "salesforce12roVyGLcl6OAh94SY2wHcoOiZx";
private static final String clientSecret = "8699127885713399099";
private String authToken="NULL";
private String authURL = 
  "https://login.salesforce.com/services/oauth2/token";
private String baseURL = "https://na22.salesforce.com/services/data/v20.0";
private String queryURL = baseURL+"/query";
private String catalogURL = baseURL+"/sobjects";

private QueryResult queryResult = null;

public SFExecutor() {
	
	
	System.setProperty("https.proxyHost", "www-proxy.us.oracle.com");
	System.setProperty("https.proxyPort", "80");
	System.setProperty("http.proxyHost", "www-proxy.us.oracle.com");
	System.setProperty("http.proxyPort", "80");
	refreshAuthToken();
	
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
    params.add("client_id", clientID);
    params.add("client_secret", clientSecret);
    params.add("username", userName);
    params.add("password", password);

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

