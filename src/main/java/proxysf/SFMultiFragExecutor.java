package proxysf;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;

import org.springframework.util.MultiValueMap;

public class SFMultiFragExecutor extends SFExecutor {

	

	public SFMultiFragExecutor(MultiValueMap<String, String> headers, String denv) throws ProcessingException {
		super(headers, denv);
	
	}
	
	@Override
	public QueryResult executeQuery(String query, String reqTypes,
			String nextMarker, String url) throws ProcessingException 
	{
		if (nextMarker == "" || nextMarker.equalsIgnoreCase(""))
			return getQueryFrag1(query, reqTypes, url);
		else
			return getQueryFrag2(query, reqTypes, nextMarker);
	}

	private QueryResult getQueryFrag2(String query, String reqTypes, String nextMarker) {
		
		QueryResult qr = null;
		try {
			qr = deserializeResultFromFile(nextMarker);
		} catch (ClassNotFoundException | IOException e) {
			throw new RuntimeException("error deserializing query result for " + nextMarker);
		}
		return qr;
	}

	private QueryResult deserializeResultFromFile(String nextMarker) throws IOException, ClassNotFoundException {
		
		String fileName = getFileName(nextMarker);
		FileInputStream fis = null;
		try {
			 fis = new FileInputStream(fileName);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Invalid file/marker " + nextMarker + " file: " + fileName);
			
		}
		
		ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(fis));
		Object o = ois.readObject();
		ois.close();
		return (QueryResult )o;
	}

	private QueryResult getQueryFrag1(String query, String reqTypes, String url) throws ProcessingException {
		QueryResult r = super.executeQuery(query, reqTypes, "", url);
		String nextMarker = UUID.randomUUID().toString();
		
		
		try {
			serializeResultToFile(r, nextMarker);
		} catch (IOException e) {
			throw new RuntimeException("exception serializing query result " + e.toString());
		}
		r.setNextUrl(url+"?nextMarker="+nextMarker);
		return r;
	}

	private void serializeResultToFile(QueryResult r, String nextMarker) throws IOException {
		String fileName = getFileName(nextMarker);
		
		FileOutputStream fos = new FileOutputStream(fileName);
		ObjectOutputStream os = new ObjectOutputStream(new BufferedOutputStream(fos));
		os.writeObject(r);
		os.flush();
		os.close();
		
	}

	public String getFileName(String nextMarker) {
		String fileName = "/scratch/ssvemuri/results/"+nextMarker+".result";
		return fileName;
	}
}
