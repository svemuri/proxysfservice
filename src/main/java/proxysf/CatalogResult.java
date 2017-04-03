package proxysf;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;



public class CatalogResult {
	

	protected static Map<String, String> typeMap = new HashMap<String,String>();
	
	static {
		typeMap.put("double", "Float64");
		typeMap.put("string", "String");
		typeMap.put("id", "String");
		typeMap.put("boolean", "String");
		typeMap.put("date", "Date");
		typeMap.put("datetime", "Timestamp");
		typeMap.put("int", "Int64");
		typeMap.put("integer", "Int64");
		typeMap.put("number", "Int64");
		typeMap.put("time", "Time");
		typeMap.put("base64", "ByteArray");
		typeMap.put("byte", "ByteArray");
		typeMap.put("anytype", "ByteArray");
		
	}
	
	public static ListingResult getTables(String sobjectsJson) {
		ObjectMapper objMapper = new ObjectMapper();
		try {
			List<CatalogItem> result = new ArrayList<CatalogItem>();
			JsonNode n = (JsonNode) objMapper.readValue(sobjectsJson, JsonNode.class);
			ArrayNode anode = (ArrayNode) n.get("sobjects");
			for (Iterator<JsonNode> it = anode.getElements(); it.hasNext();){
				JsonNode e = it.next();
				if (e!= null && e.get("name") != null)
					result.add(new CatalogItem(e.get("name").asText(), new ArrayList<LinkDescriptor>()));
			}
			return new ListingResult(result);
					
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static class CatalogItem {
		public CatalogItem(List<LinkDescriptor> links, String name,
				Map<String, String> attrs) {
			super();
			this.links = links;
			this.name = name;
			this.attrs = attrs;
		}
		public Map<String, String> getAttrs() {
			return attrs;
		}
		public CatalogItem( String name,List<LinkDescriptor> links) {
			super();
			this.links = links;
			this.name = name;
		}
		public String getName() {
			return name;
		}
		public List<LinkDescriptor> getLinks() {
			return links;
		}
		public String name;
		public List<LinkDescriptor> links;
		public Map<String, String> attrs = null;
	}
	
	public static class ListingResult {
		public ListingResult(List<CatalogItem> items) {
			super();
			this.items = items;
		}

		public List<CatalogItem> getItems() {
			return items;
		}

		private List<CatalogItem> items;
		
	}
	
	public static class LinkDescriptor {
		public LinkDescriptor(String rel, String href) {
			super();
			this.rel = rel;
			this.href = href;
		}
		public String rel = null;
		public String href = null;
		public String getRel() {
			return rel;
		}
		public String getHref() {
			return href;
		}
		
	}
	
	public static CollectionSchema getTableColumns(String tableName,
				                                   String descJson)
	{
		ObjectSchema oschema = new ObjectSchema(tableName);
		
		JsonNode n = null;
		try {
			n = new ObjectMapper().readValue(descJson, JsonNode.class);
			ArrayNode anode = (ArrayNode) n.get("fields");
			if (anode == null || anode.size() == 0)
				return null;
			for (Iterator<JsonNode> it = anode.getElements(); it.hasNext();){
				JsonNode e = it.next();
				String xsdType = e.get("soapType").asText();
				xsdType = xsdType.split(":")[1].toLowerCase();
				String colType = convertType(xsdType);
				if (e!= null && e.get("name") != null)
				{
					FieldSchema fs = new FieldSchema("", colType);
					oschema.addFieldSchema(e.get("name").getTextValue(), fs);
					System.out.println("table column desc = " + e.get("name").getTextValue() + " " + colType);
					
				}
			}
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
		
		return new CollectionSchema(new ArraySchema(oschema));
	}
	
	private static String convertType(String xsdType) {
		return typeMap.get(xsdType);
	}

	public static class CollectionSchema implements Serializable{
		public Map<String, ArraySchema> properties = null;
		
		public CollectionSchema(ArraySchema aschema)
		{
			properties = new HashMap<String, ArraySchema>();
			properties.put("items", aschema);
		}
		
		public Map<String, ArraySchema> getProperties(){
			return properties;
		}
	}
	
	public static class ArraySchema implements Serializable {
		public static String type = "array";
		public ObjectSchema items = null;
		
		
		public ArraySchema(ObjectSchema objSchema){
			this.items = objSchema;
		}
		
		public String getType() {
			return "array";
		}
		
		public ObjectSchema getItems() {
			return items;
		}
		
	}
	
	public static class ObjectSchema implements Serializable {
		public String getTitle() {
			return title;
		}



		public Map<String, FieldSchema> getProperties() {
			return properties;
		}



		public String title = null;
		public Map<String, FieldSchema> properties = null;

		public ObjectSchema(String title){
			this.title = title;
			properties = new LinkedHashMap<String, FieldSchema>();
		}

		public void addFieldSchema(String fieldName, FieldSchema fs){
			properties.put(fieldName, fs);
		}

	}

	
		
	public static class FieldSchema  implements Serializable{
		public String getType() {
			return type;
		}
		public String getDescription() {
			return description;
		}
		public FieldSchema(String description, String type) {
			super();
			this.description = description;
			this.type = type;
		}
		
		public String type = null;
		public String description = null;
		
	}
	
	
}
