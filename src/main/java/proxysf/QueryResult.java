package proxysf;

import proxysf.CatalogResult.ArraySchema;
import proxysf.CatalogResult.CollectionSchema;
import proxysf.CatalogResult.FieldSchema;
import proxysf.CatalogResult.LinkDescriptor;
import proxysf.CatalogResult.ObjectSchema;

import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;



public class QueryResult implements Serializable{
	
	public CollectionSchema getMetadata() {
		return metadata;
	}

	public List<Map<String, String>> getItems() {
		return items;
	}

	public List<LinkDescriptor> getLinks() {
		return links;
	}
	
	public QueryResult(){
		initialize();
	}

	public void initialize() {
		items = new ArrayList<Map<String,String>>();
		links = new ArrayList<LinkDescriptor>();
	}
	
	

	
	
	private CollectionSchema metadata = null;
	private List<Map<String, String>> items = null;
	private List<LinkDescriptor> links = null;
	private Map<String, String> m_outputSchema;
	
	
	
	
	/*
	public String getColumnNames(){
		String result = "";
		for (String fn: fieldNames)
			result = result+"," +fn;
		return result;
	}
	
	public String getRecords() {
		
		String result = "";
		for (String fn: recordValues)
			result = result+"," +fn;
		return result;
	}
	*/

	public void setNextUrl(String nextUrl)
	{
		this.links.add(new LinkDescriptor("next", nextUrl));
	}
	
	private void getFieldNames(JsonNode records, String pathName, List<String> fieldNames){
		ArrayNode r = (ArrayNode) records;
		JsonNode firstRecord = r.get(0);
		if (firstRecord == null)
			return;
		if (firstRecord.isObject())
			getFieldNamesFromObjectNode(firstRecord, pathName, fieldNames);
		return;
		
	}
	
	private String getFullFieldName(String prefix, String fieldName){
		return (prefix.equals("")? fieldName : prefix+"."+fieldName);
	}
	
	private void getFieldNamesFromObjectNode(JsonNode node, String pathName, List<String> fieldNames) {
		for (Iterator<String> it = node.getFieldNames(); it.hasNext();){
			String en = it.next();
			JsonNode n = node.get(en);
			
			if (n.isValueNode() || n.isMissingNode()) {
				String typeName = "String";
				String fieldName = getFullFieldName(pathName, en);
				if (fieldName.startsWith("attributes"))
					continue;
				if (m_outputSchema != null && !m_outputSchema.isEmpty() &&
						m_outputSchema.containsKey(fieldName))
					typeName = m_outputSchema.get(fieldName);
				fieldNames.add(typeName + " "+ fieldName);
			}
			else if (n.isObject())
				getFieldNamesFromObjectNode(n, getFullFieldName(pathName,en), fieldNames);
		}
			
			
		
	}

	

	public QueryResult(String resultStr, Map<String, String> outputSchema) {
		initialize();
		ObjectMapper mapper = new ObjectMapper();
		m_outputSchema = outputSchema;
		List<String> fieldNames = new ArrayList<String>();
		ObjectSchema rowSchema = new ObjectSchema("Row Schema");
		
		try {
			JsonNode node = (JsonNode) mapper.readValue(resultStr, JsonNode.class);
			getFieldNames(node.get("records"), "", fieldNames);
			
			
			// special handling for queries that return empty result
			if (m_outputSchema != null && fieldNames.size() == 0 && m_outputSchema.size() !=0)
			{
				for (String k: m_outputSchema.keySet())
					fieldNames.add(m_outputSchema.get(k) + " " +k);
			}
			createObjectSchema(rowSchema, fieldNames);
			this.metadata = new CollectionSchema(new ArraySchema(rowSchema));
			
			getRecordValues(node.get("records"), "");
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

	private void createObjectSchema(ObjectSchema rowSchema,
			                        List<String> fieldNames) 
	{
		for (String s: fieldNames){
			String[] splits = s.split("\\s+");
			rowSchema.addFieldSchema(splits[1], new FieldSchema("",splits[0]));
		}
		
	}

	private void getRecordValues(JsonNode jsonNode, String string) {
		ArrayNode n = (ArrayNode) jsonNode;
		if (n == null) return;
		for (Iterator<JsonNode> it = n.getElements(); it.hasNext();){
			JsonNode e = it.next();
			if (e == null) continue;
			addRecord(e);
		}
		
	}

	private void addRecord(JsonNode node) {
		// TODO Auto-generated method stub
		boolean first = true;
		
		Map<String, String> r = new LinkedHashMap<String,String>();
		for (Iterator<String> it = node.getFieldNames(); it.hasNext();){
			String en = it.next();
			
			if (en.equalsIgnoreCase("attributes"))
				continue;
			JsonNode n = node.get(en);
			if (n.isValueNode() || n.isMissingNode()) {
				r.put(en,(n.isNull() ? null : n.asText()));
				first = false;
			}
			else if (n.isObject())
				addRecord(n);
		}
		
		this.items.add(r);
	}

	
	
}
