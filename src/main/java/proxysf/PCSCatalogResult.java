package proxysf;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import proxysf.CatalogResult.TableColumnDescriptor;

public class PCSCatalogResult extends CatalogResult 
{
	
	public static String tablePathPrefix = "/License_RESTWebService/rest/1/";
	public static List<TableDescriptor> getTables(String catalogJson) 
	{
		ObjectMapper objMapper = new ObjectMapper();
		try {
			List<TableDescriptor> result = new ArrayList<TableDescriptor>();
			JsonNode n = (JsonNode) objMapper.readValue(catalogJson, JsonNode.class);
			ArrayNode anode = (ArrayNode) n.get("items");
			for (Iterator<JsonNode> it = anode.getElements(); it.hasNext();){
				JsonNode e = it.next();
				if (e!= null && e.get("DatasourceRestEndpoint") != null)
				{
					Map<String, String> attrs = new HashMap<String,String>();
					String fullName = e.get("DatasourceRestEndpoint").asText();
					String urlPath = new URL(fullName).getPath();
					String name = urlPath.substring(tablePathPrefix.length());
					attrs.put("fileType", "file");
					result.add(new TableDescriptor(name, null, attrs));
				}
			}
			return result;
					
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public static List<TableColumnDescriptor> getTableColumns(String tableName, String descJson)
	{
		List<TableColumnDescriptor> result = new ArrayList<TableColumnDescriptor>();
		JsonNode n = null;
		try 
		{
			n = new ObjectMapper().readValue(descJson, JsonNode.class);
			n = n.get("Resources");
			if (n == null) return result;
			n = n.get(tableName);
			if (n == null) return result;
			
			ArrayNode anode = (ArrayNode) n.get("attributes");
			for (Iterator<JsonNode> it = anode.getElements(); it.hasNext();)
			{
				JsonNode e = it.next();
				String name = e.get("name").asText();
				String type = e.get("type").asText();
				String etype = typeMap.get(type);
				if (etype == null)
					etype = "Binary";
				result.add(new TableColumnDescriptor(name, etype, false));
			}
			return result;
		}catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		return null;
	}		
			
		
}
