package proxysf;


import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonParseException;

import proxysf.CatalogResult.TableColumnDescriptor;
import proxysf.CatalogResult.TableDescriptor;

@RestController
public class SFController {

	@Autowired 
	ServletContext scontext;
    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();
    protected static final String DESCRIPTION = 
			  "{\"apiVersion\": \"1.0\","
			  + "\"instances\": ["
			  + "{\"name\" : \"Salesforce\","
			  + "\"properties\": {\"hasSchema\" : \"false\", \"async\" : \"false\", "
			  +                   "\"hasQuerySchema\":\"true\", "
			  +                   "\"dateFormat\":\"yyyy-MM-dd\","
			  +                   " \"dateTimeFormat\":\"yyyy-MM-dd'T'HH:mm:ss.SSS\"},"
			  + " \"instanceParams\": [\"client_secret\", \"client_id\"],"
			  + " \"connectionParams\" : [\"username\", \"password\",\"secret_token\"]},"
			  + "{\"name\" : \"Salesforcent\","
			  + "\"properties\": {\"hasSchema\" : \"false\", \"async\" : \"false\", "
			  +                   "\"hasQuerySchema\":\"false\", "
			  +                   "\"dateFormat\":\"yyyy-MM-dd\","
			  +                   " \"dateTimeFormat\":\"yyyy-MM-dd'T'HH:mm:ss.SSS\"},"
			  + " \"instanceParams\": [\"client_secret\", \"client_id\"],"
			  + " \"connectionParams\" : [\"username\", \"password\",\"secret_token\"]},"
			  + "{\"name\" : \"proxyfileservice\","
			  + "\"properties\": {\"hasSchema\" : \"false\", \"async\" : \"false\", "
			  +                   "\"hasQuerySchema\":\"false\", "
			  +                   "\"dateFormat\":\"yyyy-MM-dd\","
			  +                   "\"isFileService\":\"true\","
			  +                   " \"dateTimeFormat\":\"yyyy-MM-dd'T'HH:mm:ss.SSS\"},"
			  + " \"instanceParams\": [\"client_secret\", \"client_id\"],"
			  + " \"connectionParams\" : [\"username\", \"password\",\"secret_token\"]},"
			  + "{\"name\" : \"Salesforcessl\","
			  + "\"properties\": {\"hasSchema\" : \"false\", \"async\" : \"false\", "
			  +                   "\"hasQuerySchema\":\"true\", \"isSSLEnabled\":\"true\", "
			  +                   "\"dateFormat\":\"yyyy-MM-dd\","
			  +                   " \"dateTimeFormat\":\"yyyy-MM-dd'T'HH:mm:ss.SSS\"},"
			  + " \"instanceParams\": [\"client_secret\", \"client_id\"],"
			  + " \"connectionParams\" : [\"username\", \"password\",\"secret_token\"]}"
			  + "]}";
	
	public String describe() {
		// TODO Auto-generated method stub
		String secureEndpoint = scontext.getInitParameter("SECURE_ENDPOINT");
		System.out.println("secure end point = " + secureEndpoint);
		try {
		    ObjectNode n = (ObjectNode) new ObjectMapper().readValue(DESCRIPTION, JsonNode.class);
			ObjectNode sslNode = (ObjectNode) ((ArrayNode) n.get("instances")).get(2);
			((ObjectNode) sslNode.get("properties")).put("secureURL", secureEndpoint);
			
			 return n.toString();
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-geneprated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	private boolean isFileService(String iname){
		String isFileService = scontext.getInitParameter("IS_FILE_SERVICE");
    	if (isFileService.equalsIgnoreCase("true"))
    		return true;
    	if (iname != null && iname.equalsIgnoreCase("proxyfileservice"))
    		return true;
    	return false;
    			
	}
    private SFExecutor createExecutor(MultiValueMap<String, String> headers){
    	String iname = null;
  
    	
    	if (headers.containsKey("instanceName"))
    		iname = headers.getFirst("instanceName");
    	

    	if (isFileService(iname))
    		return new FileSystemExecutor(headers, scontext);
    	
    	if (iname == null || iname.trim().equalsIgnoreCase("salesforce"))
    		return new SFExecutor(headers);
    	else if (iname.trim().equalsIgnoreCase("salesforceuntyped") ||
    			iname.trim().equalsIgnoreCase("salesforcent"))
    		return new SFNTExecutor(headers);
    	else if (iname.trim().equalsIgnoreCase("salesforcessl"))
    		return new SFExecutor(headers);
    		
    	throw new RuntimeException("unknown instance name = " + iname);
    }
    
    
    @RequestMapping("/Query")
    public QueryResult getQueryResult(@RequestHeader MultiValueMap<String,String> headers, 
    		@RequestParam(value="q", defaultValue="World") String query,
    		@RequestParam(value="reqTypes", defaultValue="false") String reqTypes) {
    	
        return new SFExecutor(headers).executeQuery(query, reqTypes);
    }
    
    @RequestMapping("/Describe")
    public String getDescription(@RequestHeader MultiValueMap<String,String> headers) { 
    		
    	
        return  describe();
    }
    
    @RequestMapping("/Validate")
    public String validateCredentials(@RequestHeader MultiValueMap<String,String> headers) { 
    		
    	
        return  new SFExecutor(headers).validateCredentials();
    }
    
    
	@RequestMapping("/Catalog")
    public List<String> getSchemas(@RequestParam(value="schemaPattern", defaultValue="") String schemaPattern){
    	List<String> result = new ArrayList<String>();
    	result.add("srinivas.s.vemuri@gmail.com");
    	return result;
    }
    
    @RequestMapping("/Catalog/Schema")
    
    
    public List<TableDescriptor> 
    getTableSchema(@RequestHeader MultiValueMap<String,String> headers,
    		@RequestParam(value="schema", defaultValue="") String schema,
    		        @RequestParam(value="tablePattern", defaultValue="") String tablePattern,
    		        @RequestParam(value="table", defaultValue="") String tableName){
    	if (tableName== null || tableName.equalsIgnoreCase(""))
    		return createExecutor(headers).getTables(schema);
    	else 
    		return getOneTableColumns(headers, tableName);
    	
    }

	private List<TableDescriptor> getOneTableColumns(
			MultiValueMap<String, String> headers, String tableName) {
		List<TableColumnDescriptor> columns =
    			new SFExecutor(headers).getTableColumns(tableName);
    	List<TableDescriptor>  result =
    			new ArrayList<TableDescriptor>();
    	result.add(new TableDescriptor(tableName,columns));
    	return result;
	}
    
	@RequestMapping(value = "/getFile", method = RequestMethod.GET, produces = "application/text")
	public ResponseEntity<InputStreamResource> downloadFile(
			@RequestParam(value="name") String filePath)
	        throws IOException {

	    InputStream is = new BufferedInputStream(new FileInputStream(filePath));
	    return ResponseEntity
	            .ok()
	            
	            .body(new InputStreamResource(is));
	}
}


