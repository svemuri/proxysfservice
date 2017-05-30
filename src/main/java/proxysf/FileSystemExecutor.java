package proxysf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.*;
import java.nio.file.*;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletResponse;

import proxysf.CatalogResult.CatalogItem;
import proxysf.CatalogResult.ListingResult;

import org.apache.commons.io.IOUtils;
import org.springframework.util.MultiValueMap;




public class FileSystemExecutor extends SFExecutor {

	private String rootPath;
	private Map<String, List<String>> parent2childMap;
	private static String FSROOT = "fsroot";
	private static String FILE_TREE_FILE_NAME = "filetree.txt";
	
	public FileSystemExecutor(MultiValueMap<String, String> headers, ServletContext scontext, String denv) throws ProcessingException {
		super(headers, denv);
		//rootPath = scontext.getInitParameter("FILE_SYSTEM_ROOT");
		init();
	}
	
	public FileSystemExecutor(MultiValueMap<String, String> headers, String denv) throws ProcessingException {
		super(headers, denv);
		init();
	}

	public void init() {
		rootPath = FSROOT;
		parent2childMap = new HashMap<String, List<String>>();
		readFileTree();
	}

	private void readFileTree() {
		InputStream in = FileSystemExecutor.class.getClassLoader().getResourceAsStream(FILE_TREE_FILE_NAME);
 	    String fileTree = null;
 	    try {
 	      fileTree = IOUtils.toString(in, "UTF-8");
 	    }
 	    catch (IOException e) {
 	    	throw new RuntimeException("Conversion of file input stream to string failed");
 	    }
		
 	    System.out.println("read file tree = " + fileTree);
 	    fileTree = fileTree.substring(1,  fileTree.length()-1);
 	    createFileTree(fileTree);
	}

	private void createFileTree(String fileTree) {
		String[] entries = fileTree.split(",");
		for (String e: entries){
			String[] paths = e.split(":");
			if (paths.length == 1 || paths[1] == null)
				addFileTreeEntry(FSROOT,paths[0].trim());
			else
			  addFileTreeEntry(paths[1].trim(), paths[0].trim());
		}
		
	}

	private void addFileTreeEntry(String parent, String child) {
		if (parent2childMap.get(parent) == null)
			parent2childMap.put(parent, new ArrayList<String>());
		List<String> children = parent2childMap.get(parent);
		children.add(child);
		
	}

	public Object getFolderListing(String folderName) throws ProcessingException {

		List<CatalogItem> result = new ArrayList<CatalogItem>();

		if (folderName == null || folderName.equals("")) {
			HashMap<String, String> attrs = new HashMap<String, String>();
			attrs.put("type", "directory");
			return new ListingResult(
					Arrays.asList(new CatalogItem[] { new CatalogItem(null,
							rootPath, attrs) }));
		} else {
			
		
			addAllFilePaths(result, (folderName));
			return new ListingResult(result);
		}

	}

	private void addAllFilePaths(List<CatalogItem> result, String dir) throws ProcessingException {

		List<String> dirStream = null;
		dirStream = parent2childMap.get(dir);
		if (dirStream == null)
			throw new ProcessingException("not a directory " + dir);
		for (String pathName : dirStream) {
			Map<String, String> attrs = new HashMap<String, String>();
			if (!(pathName.endsWith("csv") || pathName.endsWith("xlsx"))) {
				attrs.put("type", "directory");

				// addAllFilePaths(result, pathName);
			} else {
				attrs.put("type", "file");

			}
			result.add(new CatalogItem(null, pathName, attrs));
		}
	}

	/*
	public InputStream getFile(String filePath) throws IOException {

		filePath = rootPath + "/" + filePath;
		InputStream in = FileSystemExecutor.class.getClassLoader()
				.getResourceAsStream(filePath);
		return new BufferedInputStream(in);
	}
	*/
	
	public void getFile(String filePath, HttpServletResponse response) throws IOException {

		filePath = rootPath + "/" + filePath;
		InputStream in = FileSystemExecutor.class.getClassLoader()
				.getResourceAsStream(filePath);
		org.apache.commons.io.IOUtils.copy(in, response.getOutputStream());
	    response.flushBuffer();	
	}

	/*
	public static String readFile(String fileName) throws IOException {

		BufferedReader br = new BufferedReader(new FileReader(fileName));
		String result = "";
		String line;
		while ((line = br.readLine()) != null)
			result += line + "\n"; // "\n";
		return result;
	}
	*/

}
