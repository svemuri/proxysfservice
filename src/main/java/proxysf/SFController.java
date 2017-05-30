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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;

import proxysf.CatalogResult.CatalogItem;
import proxysf.CatalogResult.CollectionSchema;
import proxysf.CatalogResult.ListingResult;

import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonParseException;

@Configuration
@ComponentScan
@PropertySource("classpath:application.properties")
@EnableAutoConfiguration

@RestController
public class SFController {
	@Value("${env}")
	private String env;
	
	@Autowired 
	ServletContext scontext;
    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();
    
	private boolean isNull(String s){
		return (s == null || s.equals(""));
	}
    
	public String describe() {
		// TODO Auto-generated method stub
		String secureEndpoint = scontext.getInitParameter("SECURE_ENDPOINT");
		System.out.println("secure end point = " + secureEndpoint);
		try {
		    // ObjectNode n = (ObjectNode) new ObjectMapper().readValue(DESCRIPTION, JsonNode.class);
			ObjectNode n = readProviderInstancesJson();
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
	
	private ObjectNode readProviderInstancesJson() throws org.codehaus.jackson.JsonParseException, JsonMappingException, IOException 
	{
		InputStream in = SFController.class.getClassLoader().getResourceAsStream("provider-instances.json");
 	    String json = null;
 	    try {
 	      json = IOUtils.toString(in, "UTF-8");
 	      
 	    }
 	    catch (IOException e) {
 	    	throw new RuntimeException("Conversion of file input stream to string failed");
 	    }
 	   ObjectNode n = (ObjectNode) new ObjectMapper().readValue(json, JsonNode.class);
		return n;
	}

	private boolean isFileService(String iname)
	{
		String isFileService = scontext.getInitParameter("IS_FILE_SERVICE");
    	if (isFileService != null && isFileService.equalsIgnoreCase("true"))
    		return true;
    	System.out.println("instance name = " + iname);
    	if (iname != null && iname.equalsIgnoreCase("proxyfileservice"))
    		return true;
    	return false;
    			
	}
	
	private boolean isPCSService(String iname)
	{
		return (iname != null && iname.equalsIgnoreCase("opscs-adaptersvc"));
	}
	
	private SFExecutor createExecutor(MultiValueMap<String, String> headers,
			String iname) throws ProcessingException
    {
    	
    	

    	if (isFileService(iname))
    		return new FileSystemExecutor(headers, scontext, env);
    	
    	
    	if (iname == null || iname.trim().equalsIgnoreCase("salesforce"))
    		return new SFExecutor(headers,env);
    	else if (iname.trim().equalsIgnoreCase("salesforceuntyped") ||
    			iname.trim().equalsIgnoreCase("salesforcent"))
    		return new SFNTExecutor(headers,env);
    	else if (iname.trim().equalsIgnoreCase("SFMultiFrag"))
    		return new SFMultiFragExecutor(headers,env);
    	else if (iname.trim().equalsIgnoreCase("salesforcessl"))
    		return new SFExecutor(headers,env);
    		
    	throw new RuntimeException("unknown instance name = " + iname);
    }

    private FileSystemExecutor getFileSystemExecutor(
    		MultiValueMap<String, String> headers,
    		String iname) throws ProcessingException{
    	

    	if (isFileService(iname))
    		return new FileSystemExecutor(headers, scontext,env);
    	
    	throw new RuntimeException("Not a fie system instance " + iname );
    	
    }
    
	
    
    
    @RequestMapping("{instanceName}/{version}/Query")
    public QueryResult getQueryResult(
    		@PathVariable("instanceName") String instanceName,
    		@RequestHeader MultiValueMap<String,String> headers, 
    		@RequestParam(value="q", defaultValue="World") String query,
    		@RequestParam(value="reqTypes", defaultValue="false") String reqTypes,
    		@RequestParam(value="nextMarker", defaultValue="") String nextMarker,
    		HttpServletRequest request) throws ProcessingException 
    {
    	
    	String url = request.getRequestURL().toString();
        return createExecutor(headers, instanceName).executeQuery(query, reqTypes, nextMarker, url);
    }
    
    @RequestMapping("/")
    public String getDescription0(@RequestHeader MultiValueMap<String,String> headers) { 
    		
    	
        return  describe();
    }
   
    @RequestMapping("/Describe")
    public String getDescription(@RequestHeader MultiValueMap<String,String> headers) { 
    		
    	
        return  describe();
    }
    
    @RequestMapping(value = "{instanceName}/{version}/Validate")
    public String validateCredentials(@RequestHeader MultiValueMap<String,String> headers) throws ProcessingException { 
    		
    	
        return  new SFExecutor(headers,env).validateCredentials();
    }
    
    @RequestMapping(value = "{instanceName}/{version}/metadata-catalog")
    public Object metadataCatalogRootQuery(
    		@PathVariable("instanceName") String instanceName,
    		
    		@RequestHeader MultiValueMap<String,String> headers)
    {
    	return getTopSchemas();
    }
    		
	@RequestMapping(value = "{instanceName}/{version}/metadata-catalog/{schemaName:.+}")
    public Object metadataCatalogListObjects(
    		@PathVariable("instanceName") String instanceName,
    		@PathVariable(value="schemaName") String schemaName,
    		@RequestHeader MultiValueMap<String,String> headers) throws ProcessingException
    		
	{
		return createExecutor(headers, instanceName).getTables(schemaName);
    	
	}	
    
	@RequestMapping(value = "{instanceName}/{version}/metadata-catalog/object-schema")
    public Object metadataCatalogGetObjectSchema(
    		@PathVariable("instanceName") String instanceName,
    		@RequestParam(value="schema", defaultValue="") String schemaName,
    		@RequestParam("name") String objectName,
    		@RequestHeader MultiValueMap<String,String> headers) throws ProcessingException
    		
	{
		return getOneTableColumns(headers,instanceName,objectName);
	}	
	
	@RequestMapping(value = "{instanceName}/{version}/metadata-catalog/list")
    public Object getFolderListing(
    		@PathVariable("instanceName") String instanceName,
    		
    		@RequestParam(name="name", defaultValue="") String foldertName,
    		@RequestHeader MultiValueMap<String,String> headers) throws ProcessingException
    		
	{
		if (isFileService(instanceName) || isPCSService(instanceName))
			return getFileSystemExecutor(headers, instanceName).getFolderListing(foldertName);
		throw new RuntimeException("Listing not supported for " + instanceName);
	}
	
		
	private ListingResult getTopSchemas()
	{
		List<CatalogItem> result = new ArrayList<CatalogItem>();
    	result.add(new CatalogItem("RESTADAPTER_SCHEMA", null));
    	return new ListingResult(result);
    }
	
	

	private CollectionSchema getOneTableColumns(
			MultiValueMap<String, String> headers,
			String instanceName,
			String tableName) throws ProcessingException 
	{
		CollectionSchema columns =
    			createExecutor(headers, instanceName).getTableColumns(tableName);
    	
    	return columns;
	}
    
	/*
	@RequestMapping(value = "{instanceName}/{version}", method = RequestMethod.GET, produces = "application/text")
	public ResponseEntity<InputStreamResource> downloadFile(
			@PathVariable("instanceName") String instanceName,
			@RequestParam(value="name", defaultValue="") String filePath,
			
			@RequestHeader MultiValueMap<String,String> headers)
			
			
	        throws IOException, ProcessingException 
	{

		
	    InputStream is = createExecutor(headers, instanceName).getFile(filePath);
	    return ResponseEntity
	            .ok()
	            
	            .body(new InputStreamResource(is));
	}
	*/
	
	@RequestMapping(value = "{instanceName}/{version}", method = RequestMethod.GET, produces = "application/text")
	public void downloadFile(
			@PathVariable("instanceName") String instanceName,
			@RequestParam(value="name", defaultValue="") String filePath,
			
			@RequestHeader MultiValueMap<String,String> headers,
			HttpServletResponse response)
			
	        throws IOException, ProcessingException 
	{

		
	     createExecutor(headers, instanceName).getFile(filePath, response);
	    
	}
	
	@ExceptionHandler(ProcessingException.class)

	public ResponseEntity<ErrorResponse> exceptionHandler(Exception ex) {

		ErrorResponse error = new ErrorResponse();

		error.setErrorCode(HttpStatus.PRECONDITION_FAILED.value());

		error.setMessage(ex.getMessage());

		return new ResponseEntity<ErrorResponse>(error, HttpStatus.OK);

	}
	 
	public static class ErrorResponse {

		private int errorCode;

		private String message;

		public int getErrorCode() {

			return errorCode;

		}

		public void setErrorCode(int errorCode) {

			this.errorCode = errorCode;

		}

		public String getMessage() {

			return message;

		}

		public void setMessage(String message) {

			this.message = message;

		}

	}
}


