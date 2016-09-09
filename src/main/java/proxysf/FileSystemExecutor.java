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
		
	}


