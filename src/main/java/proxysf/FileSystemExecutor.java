package proxysf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.*;
import java.nio.file.*;

import javax.servlet.ServletContext;

import proxysf.CatalogResult.CatalogItem;
import proxysf.CatalogResult.ListingResult;
import org.springframework.util.MultiValueMap;




public class FileSystemExecutor extends SFExecutor {

	private String rootPath;

	public FileSystemExecutor(MultiValueMap<String, String> headers, ServletContext scontext) throws ProcessingException {
		super(headers);
		rootPath = scontext.getInitParameter("FILE_SYSTEM_ROOT");
		// TODO Auto-generated constructor stub
	}
	
	public FileSystemExecutor(MultiValueMap<String, String> headers) throws ProcessingException {
		super(headers);
	}

	public Object getFolderListing(String folderName) {
		
		List<CatalogItem> result = new ArrayList<CatalogItem>();
		
		/*
		Iterable<Path> rootdirs = FileSystems.getDefault().getRootDirectories();
		for (Path dir: rootdirs){
			addAllFilePaths(result, dir);
		}
		*/
		if (folderName == null || folderName.equals(""))
		{
			HashMap<String, String> attrs = new HashMap<String, String>();
			attrs.put("type", "directory");
			return new ListingResult(Arrays.asList(new CatalogItem[]{new CatalogItem(null,"FS-ROOT", attrs)}));
		}
		else
		{
			if (folderName.equals("FS-ROOT"))
				folderName = rootPath;
			else
				folderName = rootPath + "/"+folderName;
			addAllFilePaths(result, Paths.get(folderName));
			return new ListingResult(result);
		}
		
		
	}

	private void addAllFilePaths(List<CatalogItem> result, Path dir) {
		
		DirectoryStream<Path> dirStream = null;
		try {
			dirStream = Files.newDirectoryStream(dir);

			for (Path pathName: dirStream){
				Map<String, String> attrs = new HashMap<String,String>();
				if (Files.isDirectory(pathName)){
					attrs.put("type", "directory");

					//addAllFilePaths(result, pathName);
				}
				else {
					attrs.put("type", "file");

				}
				result.add(new CatalogItem(null,Paths.get(rootPath).relativize(pathName).toString(),  attrs));
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
			objName = rootPath + "/" + objName;
			return getObject(objName);
		}
		filePath = rootPath + "/" + filePath;
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


