package proxysf;

import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;



public class QueryResult{
	
	
	private List<String> fieldNames = new ArrayList<String>();
	private List<String> recordValues = new ArrayList<String>();
	
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
			if (n.isValueNode() || n.isMissingNode())
				fieldNames.add("String "+ getFullFieldName(pathName, en));
			else if (n.isObject())
				getFieldNamesFromObjectNode(n, getFullFieldName(pathName,en));
		}
			
			
		
	}

	

	public QueryResult(String resultStr) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			JsonNode node = (JsonNode) mapper.readValue(resultStr, JsonNode.class);
			getFieldNames(node.get("records"), "");
			
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
		for (Iterator<JsonNode> it = n.getElements(); it.hasNext();){
			JsonNode e = it.next();
			addRecord(e);
		}
		
	}

	private void addRecord(JsonNode node) {
		// TODO Auto-generated method stub
		boolean first = true;
		for (Iterator<String> it = node.getFieldNames(); it.hasNext();){
			String en = it.next();
			JsonNode n = node.get(en);
			if (n.isValueNode() || n.isMissingNode()) {
				recordValues.add(n.toString());
				first = false;
			}
			else if (n.isObject())
				addRecord(n);
		}
			
	}
	
}
