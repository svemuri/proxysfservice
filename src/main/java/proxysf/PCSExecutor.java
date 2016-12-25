package proxysf;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import javax.ws.rs.core.MultivaluedMap;
import javax.xml.bind.DatatypeConverter;















import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.springframework.util.MultiValueMap;

import proxysf.CatalogResult.TableColumnDescriptor;
import proxysf.CatalogResult.TableDescriptor;

import com.fasterxml.jackson.core.JsonParseException;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;


public class PCSExecutor extends SFExecutor {
	String basicAuth = null;
	String catalogURL = null;
	String queryURL = null;
	
	public PCSExecutor(MultiValueMap<String, String> headers) {
		super(headers);
		setConnectionJsonParams(headers);
		catalogURL = credentialsMap.get("catalogURL");
		queryURL = credentialsMap.get("queryURL");
	}
	
	private void createBasicAuthHeader() {
		String username = credentialsMap.get("username");
		String password = credentialsMap.get("password");
		basicAuth = DatatypeConverter.printBase64Binary((username+":"+password).getBytes());
		
	}

	public List<TableDescriptor> getTables(String schema) {
		String catalogJson = restCall(catalogURL, null);
		return PCSCatalogResult.getTables(catalogJson);
	}
	
	public InputStream getFile(String objName) throws IOException
	{
		MultivaluedMap<String, String> params = new MultivaluedMapImpl();
		
		System.out.println("PCS rest call to " + queryURL+"/"+objName);
		String resultJson = restCall(queryURL+"/" + objName,null);
		return getQueryResultStream(resultJson);
	}
	
	private InputStream getQueryResultStream(String resultJson) {
		try 
		{
			ObjectMapper mapper = new ObjectMapper();
			JsonNode node = (JsonNode) mapper.readValue(resultJson, JsonNode.class);
			ArrayNode a = (ArrayNode) node.get("items");
			if (a == null) return null;
			
			StringBuffer sb = new StringBuffer();
			boolean first = true;
			for (Iterator<JsonNode> it = a.getElements(); it.hasNext();){
				JsonNode e = it.next();
				if (e == null) continue;
		
				addRecord(sb,e, first);
				first = false;
			}
			String qresult = sb.toString();
			
			System.out.println("PCS query result (csv) = " + qresult);
			return new ByteArrayInputStream(qresult.getBytes("UTF-8"));
		}
		catch (JsonParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (JsonMappingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return null;
		
		
	}

	private void addRecord(StringBuffer sb, JsonNode e, boolean getNames) 
	{
		String columnNames = "";
		String columnValues = "";
		boolean isFirst = true;
		for (Iterator<String> it = e.getFieldNames(); it.hasNext();)
		{
			String en = it.next();
			if (en.equals("links"))
				continue;
			if (getNames)
				columnNames += (isFirst ? "" : ",") + en;
			JsonNode v = e.get(en);
			columnValues += (isFirst ? "" : ",") + (v == null ? "" : v.asText());
			isFirst = false;
		}
		if (getNames)
			sb.append(columnNames + "\n");
		else
			sb.append("\n");
		sb.append(columnValues);
	
	}

	protected void setConnectionJsonParams(MultiValueMap<String, String> headers) {
		String connection_json_param = headers.getFirst("connection_json");
		
		String connection_json = new String(DatatypeConverter.parseBase64Binary(connection_json_param));
		ConnectionJsonParser.parse(connection_json, credentialsMap);
		createBasicAuthHeader();
	}
	
	
	protected String restCall(String endPoint, MultivaluedMap<String, String> params) {


		Client client = Client.create();

		System.out.println("Basic auth header = " + basicAuth);
		System.out.println("Rest call invoked to " + endPoint);
		WebResource webResource = client.resource(endPoint);

		if (params != null)
			webResource = webResource.queryParams(params);
		ClientResponse response =  webResource
				.accept("application/json")
				.header("Authorization", "Basic "+basicAuth)
				.get(ClientResponse.class);


		if (response.getStatus() != 200) {
			System.out.println("error response = " + response.getEntity(String.class));
			throw new RuntimeException("Failed trying to execute query: HTTP error code : "
					+ response.getStatus());
		}

		String resultStr = response.getEntity(String.class);
		System.out.println("Query result from PCS = " + resultStr);
		return resultStr;
	}

	public List<TableColumnDescriptor> getTableColumns(String tableName) 
	{
		
		String descJson = restCall(queryURL+"/"+tableName+"/describe", null);
		return PCSCatalogResult.getTableColumns(tableName,descJson);
	}

}
