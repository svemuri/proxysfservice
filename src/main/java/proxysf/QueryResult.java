package proxysf;

import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;



public class QueryResult{
	
	
	private List<String> fieldNames = new ArrayList<String>();
	private List<String> recordValues = new ArrayList<String>();
	private Map<String, String> m_outputSchema;
	
	public List<String> getColumnNames(){
		return fieldNames;
	}
	
	public List<String> getRecords() {
		return recordValues;
	}
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
	
	private void getFieldNames(JsonNode records, String pathName){
		ArrayNode r = (ArrayNode) records;
		JsonNode firstRecord = r.get(0);
		if (firstRecord == null)
			return;
		if (firstRecord.isObject())
			getFieldNamesFromObjectNode(firstRecord, pathName);
		return;
		
	}
	
	private String getFullFieldName(String prefix, String fieldName){
		return (prefix.equals("")? fieldName : prefix+"."+fieldName);
	}
	
	private void getFieldNamesFromObjectNode(JsonNode node, String pathName) {
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
				getFieldNamesFromObjectNode(n, getFullFieldName(pathName,en));
		}
			
			
		
	}

	

	public QueryResult(String resultStr, Map<String, String> outputSchema) {
		ObjectMapper mapper = new ObjectMapper();
		m_outputSchema = outputSchema;
		try {
			JsonNode node = (JsonNode) mapper.readValue(resultStr, JsonNode.class);
			getFieldNames(node.get("records"), "");
			
			// special handling for queries that return empty result
			if (m_outputSchema != null && fieldNames.size() == 0 && m_outputSchema.size() !=0)
			{
				for (String k: m_outputSchema.keySet())
					fieldNames.add(m_outputSchema.get(k) + " " +k);
			}
			
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
		for (Iterator<String> it = node.getFieldNames(); it.hasNext();){
			String en = it.next();
			
			if (en.equalsIgnoreCase("attributes"))
				continue;
			JsonNode n = node.get(en);
			if (n.isValueNode() || n.isMissingNode()) {
				recordValues.add((n.isNull() ? null : n.asText()));
				first = false;
			}
			else if (n.isObject())
				addRecord(n);
		}
			
	}
	
}
