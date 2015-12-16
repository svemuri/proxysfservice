package hello;


import hello.CatalogResult.TableColumnDescriptor;
import hello.CatalogResult.TableDescriptor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SFController {

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();

    @RequestMapping("/Query")
    public QueryResult getQueryResult(@RequestParam(value="q", defaultValue="World") String query) {
        return new SFExecutor().executeQuery(query);
    }
    
    @RequestMapping("/Catalog")
    public List<String> getSchemas(@RequestParam(value="schemaPattern", defaultValue="") String schemaPattern){
    	List<String> result = new ArrayList<String>();
    	result.add("srinivas.s.vemuri@gmail.com");
    	return result;
    }
    
    @RequestMapping("/Catalog/Schema")
    
    
    public List<TableDescriptor> 
    getTableSchema(@RequestParam(value="schema", defaultValue="") String schema,
    		        @RequestParam(value="tablePattern", defaultValue="") String tablePattern,
    		        @RequestParam(value="table", defaultValue="") String tableName){
    	if (tableName== null || tableName.equalsIgnoreCase(""))
    		return new SFExecutor().getTables(schema);
    	
    	List<TableColumnDescriptor> columns =
    			new SFExecutor().getTableColumns(tableName);
    	List<TableDescriptor>  result =
    			new ArrayList<TableDescriptor>();
    	result.add(new TableDescriptor(tableName,columns));
    	return result;
    	
    }
    
}


