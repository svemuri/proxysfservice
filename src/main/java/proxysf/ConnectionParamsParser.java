package proxysf;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

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
import java.util.Map;

public class ConnectionParamsParser {
	public static void parseFile(String fileName) throws IOException{
		File fXmlFile = new File(fileName);
		String sb = readFile(fileName);
		parse(sb, null);
	}
	
	private static String readFile(String fileName) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		String result = "";
		String line;
		while ((line = br.readLine()) != null)
			result += line + "\n";
		return result;
	}

	public static void parse(String content, Map<String, String> headerParams){
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		try {
			System.out.println("proxysf: parsing connection xml:" + content);
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = 
					dBuilder.parse(new ByteArrayInputStream(content.getBytes("UTF-8")));
			parseConnectionParams(doc, headerParams);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}

	private static void parseConnectionParams(Document doc, Map<String, String> headerParams) {
		// TODO Auto-generated method stub
		doc.getDocumentElement().normalize();

		System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
			
		//setProperty(doc, "cm:instanceName", headerParams);
		extractParams(doc, "cm:instanceParams", headerParams);
		extractParams(doc, "cm:connectionParams", headerParams);
	  

	}

	private static void setProperty(Document doc, String tagName, 
			Map<String, String> headerParams) {
		// TODO Auto-generated method stub
		NodeList nl = doc.getElementsByTagName("*");
		for (int i=0; i < nl.getLength(); i++){
			Element e = (Element) (nl.item(i));
			if (!e.getTagName().equals(tagName))
				continue;
			headerParams.put(tagName, e.getTextContent());
			System.out.println("setting property " + tagName + " to " + 
					e.getTextContent());
		}
	}

	private static void extractParams(Document doc, String typeName, Map<String, String> headerParams) {
	   // printDoc(doc);
		
		
		NodeList nList = doc.getElementsByTagName("*");
				
		System.out.println("----------------------------");

		for (int temp = 0; temp < nList.getLength(); temp++) {

			Node nNode = nList.item(temp);
					
			// System.out.println("\nCurrent Element :" + nNode.getNodeName());
					
			if (nNode.getNodeType() == Node.ELEMENT_NODE) {

				Element eElement = (Element) nNode;
				if (!eElement.getTagName().equals(typeName))
					continue;

				NodeList params = eElement.getElementsByTagName("cm:param");
				for (int i=0; i < params.getLength(); i++){
					Element param = (Element) params.item(i);
					parseParameter(param, headerParams);
				}
				

			}
		}
	}

	private static void printDoc(Document doc) {
		NodeList nl = doc.getElementsByTagName("*");
		for (int i=0; i < nl.getLength(); i++){
			Element e = (Element) (nl.item(i));
			System.out.println("doc element tag = " + e.getTagName());
		}
		
	}

	private static void parseParameter(Element param, Map<String, String> headerParams) {
		String name = param.getElementsByTagName("cm:name").item(0).getTextContent();
		String value = param.getElementsByTagName("cm:value").item(0).getTextContent();
		System.out.println("Parameter Name = " + name);
		System.out.println("Parameter Value = " + value );
		if (headerParams == null)
			return;
		headerParams.put(name.trim(), value.trim());
	}

	public static void extractConnectionParams(String connectionXml, Map<String, String> headerParams) {
		// TODO Auto-generated method stub
		parse(connectionXml, headerParams);
		
	}
}


