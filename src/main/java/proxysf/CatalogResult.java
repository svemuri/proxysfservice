package proxysf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;



public class CatalogResult {
	private static Map<String, String> typeMap = new HashMap<String,String>();
	
	static {
		typeMap.put("double", "Float64");
		typeMap.put("string", "String");
		typeMap.put("id", "String");
		typeMap.put("boolean", "Bool");
		typeMap.put("date", "Date");
		typeMap.put("datetime", "Timestamp");
		typeMap.put("int", "Int64");
		typeMap.put("time", "Time");
		typeMap.put("base64", "Binary");
		typeMap.put("byte", "Byte");
		typeMap.put("anytype", "Binary");
		
	}
	
	public static List<TableDescriptor> getTables(String sobjectsJson) {
		ObjectMapper objMapper = new ObjectMapper();
		try {
			List<TableDescriptor> result = new ArrayList<TableDescriptor>();
			JsonNode n = (JsonNode) objMapper.readValue(sobjectsJson, JsonNode.class);
			ArrayNode anode = (ArrayNode) n.get("sobjects");
			for (Iterator<JsonNode> it = anode.getElements(); it.hasNext();){
				JsonNode e = it.next();
				if (e!= null && e.get("name") != null)
					result.add(new TableDescriptor(e.get("name").asText(), new ArrayList<TableColumnDescriptor>()));
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

	public static List<TableColumnDescriptor> getTableColumns(String descJson){
		List<TableColumnDescriptor> result = new ArrayList<TableColumnDescriptor>();
		
		JsonNode n = null;
		try {
			n = new ObjectMapper().readValue(descJson, JsonNode.class);
			ArrayNode anode = (ArrayNode) n.get("fields");
			if (anode == null || anode.size() == 0)
				return result;
			for (Iterator<JsonNode> it = anode.getElements(); it.hasNext();){
				JsonNode e = it.next();
				String xsdType = e.get("soapType").asText();
				xsdType = xsdType.split(":")[1].toLowerCase();
				String colType = convertType(xsdType);
				if (e!= null && e.get("name") != null){
					TableColumnDescriptor td = 
							 (new TableColumnDescriptor(e.get("name").asText(),
							colType, false));
					System.out.println("table column desc = " + td.columnName + " " + td.columnType);
					result.add(td);
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
		
		return result;
	}
	
	private static String convertType(String xsdType) {
		return typeMap.get(xsdType);
	}

	public static class TableDescriptor {
		public String getTableName() {
			return tableName;
		}
		public void setTableName(String tableName) {
			this.tableName = tableName;
		}
		public List<TableColumnDescriptor> getColumns() {
			return columns;
		}
		public void setColumns(List<TableColumnDescriptor> columns) {
			this.columns = columns;
		}
		public String tableName;
		public List<TableColumnDescriptor> columns = new ArrayList<TableColumnDescriptor>();
		public TableDescriptor(String tableName,
				List<TableColumnDescriptor> columns) {
			super();
			this.tableName = tableName;
			this.columns = columns;
		}
		
	}
	
	public static class TableColumnDescriptor {
		public String getColumnName() {
			return columnName;
		}
		public void setColumnName(String columnName) {
			this.columnName = columnName;
		}
		public String getColumnType() {
			return columnType;
		}
		public void setColumnType(String columnType) {
			this.columnType = columnType;
		}
		public boolean getIsPrimaryKey() {
			return isPrimaryKey;
		}
		public void setIsPrimaryKey(boolean isPrimaryKey) {
			this.isPrimaryKey = isPrimaryKey;
		}
		public String columnName;
		public String columnType;
		public boolean isPrimaryKey;
		public TableColumnDescriptor(String columnName, String columnType,
				boolean isPrimaryKey) {
			super();
			this.columnName = columnName;
			this.columnType = columnType;
			this.isPrimaryKey = isPrimaryKey;
		}
		
	}
}
