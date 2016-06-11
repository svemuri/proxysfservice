package proxysf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.core.MultivaluedMap;
import javax.xml.bind.DatatypeConverter;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.util.MultiValueMap;

import proxysf.CatalogResult.TableColumnDescriptor;
import proxysf.CatalogResult.TableDescriptor;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.Base64;
import com.sun.jersey.core.util.MultivaluedMapImpl;


public class SFExecutor {
	private  Map<String,String> credentialsMap = new HashMap<String,String>();
	
	
	private String authToken="NULL";
	private String authURL = 
			"https://login.salesforce.com/services/oauth2/token";
	private String baseURL = "https://na22.salesforce.com/services/data/v20.0";
	protected String queryURL = baseURL+"/query";
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
		
		
		
		if (headers.containsKey("connection_json")){
			setConnectionJsonParams(headers);
			return;
		}
		
		setDefaultCredentials();
		
		
	}

	private void setConnectionJsonParams(MultiValueMap<String, String> headers) {
		String connection_json_param = headers.getFirst("connection_json");
		
		String connection_json = new String(DatatypeConverter.parseBase64Binary(connection_json_param));
		ConnectionJsonParser.parse(connection_json, credentialsMap);
		credentialsMap.put("password", credentialsMap.get("password")+credentialsMap.get("secret_token"));
	}

	

	public QueryResult executeQuery(String query, String reqTypes) {
		// TODO Auto-generated method stub
		refreshAuthToken();
		
		boolean getTypes = Boolean.parseBoolean(reqTypes);
		Map<String, String> outputSchema = null;
		if (getTypes)
			outputSchema = getQueryReturnTypes(query);
		MultivaluedMap<String, String> params = new MultivaluedMapImpl();

		params.add("q", query);

		System.out.println("Query url = " + queryURL);
		System.out.println("Received query = " + query);
		String resultStr = restCall(queryURL, params);
		System.out.println("Query output = " + resultStr);
		return new QueryResult(resultStr, outputSchema);
	}

	private Map<String, String> getQueryReturnTypes(String query) {
		Map<String, String> querySchema = new HashMap<String, String>();
		List<String> tables = new ArrayList<String>();
		List<String> queryColumnNames = new ArrayList<String>();
		
		simpleQueryParse(query, tables, queryColumnNames);
		List<List<TableColumnDescriptor>> tableSchemas =
				new ArrayList<List<TableColumnDescriptor>>();
		
		for (String t: tables){
			List<TableColumnDescriptor> tcols = getTableColumns(t);
			tableSchemas.add(tcols);
		}
		
		for (String queryColumn: queryColumnNames){
			String qcoltype = getMatchingTableColumnType(queryColumn,tables, tableSchemas);
			querySchema.put(queryColumn, qcoltype);
		}
		
		return querySchema;
	}

	private String getMatchingTableColumnType(String queryColumn,
			List<String> tables,
			List<List<TableColumnDescriptor>> tableSchemas) 
	{
		for (int i=0; i < tables.size(); i++){
			String tableName = tables.get(i);
			List<TableColumnDescriptor> ts = tableSchemas.get(i);
			TableColumnDescriptor tcd = getQualifyingMatch(tableName, ts, queryColumn);
			if (tcd != null)
				return tcd.getColumnType();
		}
		
		for (List<TableColumnDescriptor> ts: tableSchemas){
			TableColumnDescriptor tcd = getNonQualifyingMatch(ts, queryColumn);
			if (tcd != null)
				return tcd.getColumnType();
		}
		return null;
	}

	private TableColumnDescriptor getNonQualifyingMatch(
			List<TableColumnDescriptor> ts, String queryColumn) {
		
		String baseColumn = getBaseColumn(queryColumn);
		for (TableColumnDescriptor tc: ts){
			if (queryColumn.equalsIgnoreCase(tc.getColumnName()))
				return tc;
		}
		return null;
	}

	private TableColumnDescriptor getQualifyingMatch(String tableName,
			List<TableColumnDescriptor> ts, String queryColumn) {
		
		String baseColumn = getBaseColumn(queryColumn);
		for (TableColumnDescriptor tc: ts){
			if (queryColumn.equalsIgnoreCase(tableName+"."+ tc.getColumnName()))
				return tc;
		}
		return null;
	}

	private String getBaseColumn(String queryColumn) {
		return queryColumn;
	}

	private void simpleQueryParse(String query, List<String> tables,
			List<String> queryColumnNames) {
		
		String query1 = query.trim().toLowerCase();
		int fromIdx = query1.indexOf("from");
		
		String columnList = query.substring("select".length(), fromIdx).trim();
		System.out.println("query parse columns = " + columnList);
		String[] columns = columnList.split(",");
		for (String s: columns)
			queryColumnNames.add(s.trim());
		
		String endFragment = query.substring(fromIdx + "from".length()).trim();
		String tablesFragment = endFragment.split("\\s+")[0].trim();
		
		System.out.println("tablesFragment = " + tablesFragment);
		String[] tablesList;
		
				tablesList = tablesFragment.split(",");
		
		
		for (String s: tablesList)
			tables.add(s.trim());
	}

	protected String restCall(String endPoint, MultivaluedMap<String, String> params) {
		
		
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

	protected void refreshAuthToken() {
		if (!authToken.equals("NULL"))
			return;
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
		refreshAuthToken();
		String sobjectsJson = restCall(catalogURL, null);
		List<TableDescriptor> result = CatalogResult.getTables(sobjectsJson);
		return result;

	}

	public List<TableColumnDescriptor> getTableColumns(String tableName) {
		refreshAuthToken();
		String descJson = restCall(catalogURL+"/"+tableName+"/describe", null);
		return CatalogResult.getTableColumns(descJson);
	}

	

	public String validateCredentials() {
		// TODO Auto-generated method stub
		refreshAuthToken();
		return "{\"status\" : \"OK\"}";
	}


	/*
public static void main(String[] args){
	String q = "select name from ACCOUNT";
	QueryResult r = new SFExecutor(q).getQueryResult();
}foo
	 */

}

