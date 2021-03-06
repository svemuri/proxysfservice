package proxysf;

import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.springframework.util.MultiValueMap;

import com.sun.jersey.core.util.MultivaluedMapImpl;

public class SFNTExecutor extends SFExecutor {

	public SFNTExecutor(MultiValueMap<String, String> headers, String denv) throws ProcessingException {
		super(headers, denv);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public QueryResult  executeQuery(String query, String reqTypes, String nextMarker, String url) throws ProcessingException {
		// TODO Auto-generated method stub
		refreshAuthToken();
		
		boolean getTypes = Boolean.parseBoolean(reqTypes);
		Map<String, String> outputSchema = null;
		
		MultivaluedMap<String, String> params = new MultivaluedMapImpl();

		params.add("q", query);

		System.out.println("Query url = " + queryURL);
		System.out.println("Received query = " + query);
		String resultStr = restCall(queryURL, params);
		System.out.println("Query output = " + resultStr);
		return new QueryResult(resultStr, outputSchema);
	}
}
