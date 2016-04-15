package proxysf;


import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;

import org.codehaus.jackson.JsonNode;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import proxysf.CatalogResult.TableColumnDescriptor;
import proxysf.CatalogResult.TableDescriptor;

@RestController
public class SFController {

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();

    @RequestMapping("/Query")
    public QueryResult getQueryResult(@RequestHeader MultiValueMap<String,String> headers, 
    		@RequestParam(value="q", defaultValue="World") String query) {
    	
        return new SFExecutor(headers).executeQuery(query);
    }
    
    @RequestMapping("/Describe")
    public String getDescription(@RequestHeader MultiValueMap<String,String> headers) { 
    		
    	
        return  new SFExecutor(headers).describe();
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
    		return new SFExecutor(headers).getTables(schema);
    	
    	List<TableColumnDescriptor> columns =
    			new SFExecutor(headers).getTableColumns(tableName);
    	List<TableDescriptor>  result =
    			new ArrayList<TableDescriptor>();
    	result.add(new TableDescriptor(tableName,columns));
    	return result;
    	
    }
    
}


