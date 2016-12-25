package proxysf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.*;
import java.nio.file.*;

import javax.servlet.ServletContext;

import org.springframework.util.MultiValueMap;

import proxysf.CatalogResult.TableDescriptor;
import proxysf.CatalogResult.*;


public class FileSystemExecutor extends SFExecutor {

	private String rootPath;

	public FileSystemExecutor(MultiValueMap<String, String> headers, ServletContext scontext) {
		super(headers);
		rootPath = scontext.getInitParameter("FILE_SYSTEM_ROOT");
		// TODO Auto-generated constructor stub
	}
	
	public List<TableDescriptor> getTables(String schema) {
		
		List<TableDescriptor> result = new ArrayList<TableDescriptor>();
		
		/*
		Iterable<Path> rootdirs = FileSystems.getDefault().getRootDirectories();
		for (Path dir: rootdirs){
			addAllFilePaths(result, dir);
		}
		*/
		addAllFilePaths(result, Paths.get(rootPath));
		return result;
		
	}

	private void addAllFilePaths(List<TableDescriptor> result, Path dir) {
		// TODO Auto-generated method stub
		DirectoryStream<Path> dirStream = null;
		try {
			dirStream = Files.newDirectoryStream(dir);

			for (Path pathName: dirStream){
				Map<String, String> attrs = new HashMap<String,String>();
				if (Files.isDirectory(pathName)){
					attrs.put("fileType", "directory");

					addAllFilePaths(result, pathName);
				}
				else {
					attrs.put("fileType", "file");

				}
				result.add(new TableDescriptor(pathName.toString(), null, attrs));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public InputStream getFile(String filePath) throws IOException
	{
		if (filePath.startsWith("object:"))
		{
			String objName = filePath.substring("object:".length());
			System.out.println("OBJECT GET for " + objName);
			return getObject(objName);
		}
		return new BufferedInputStream(new FileInputStream(filePath));
	}
	
	public InputStream getObject(String objectName) throws IOException 
	{
		String s = readFile(objectName);
		return new BufferedInputStream(new ByteArrayInputStream(s.getBytes("UTF-8")));
	}
	
	public static String readFile(String fileName) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String result = "";
        String line;
        while ((line = br.readLine()) != null)
                result += line + "\n" ; //"\n";
        return result;
}

	}


