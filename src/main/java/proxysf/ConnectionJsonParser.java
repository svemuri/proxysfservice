package proxysf;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class ConnectionJsonParser {
	
	public static void parse(String content, Map<String, String> headerParams){
		
		try {
			System.out.println("proxysf: parsing connection xml:" + content);
			
			JsonNode connectionJson = new ObjectMapper().readValue(content, JsonNode.class);
			
			
			for (Iterator<String> fields = connectionJson.getFieldNames();
					fields.hasNext(); ){
				String f = fields.next();
				headerParams.put(f, connectionJson.get(f).getTextValue());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}

	
	public static void extractConnectionParams(String connectionXml, Map<String, String> headerParams) {
		// TODO Auto-generated method stub
		parse(connectionXml, headerParams);
		
	}
}


