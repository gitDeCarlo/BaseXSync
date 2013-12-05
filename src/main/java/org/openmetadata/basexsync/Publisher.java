package org.openmetadata.basexsync;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.basex.core.BaseXException;
import org.basex.core.Context;
import org.basex.core.cmd.Close;
import org.basex.core.cmd.CreateDB;
import org.basex.core.cmd.Delete;
import org.basex.core.cmd.List;
import org.basex.core.cmd.Open;
import org.basex.core.cmd.Optimize;
import org.basex.core.cmd.Replace;
import org.basex.core.cmd.XQuery;
import org.openmetadata.harvester.CatalogEntry;
import org.openmetadata.nesstar.HarvesterOptions;
import org.openmetadata.nesstar.NesstarUtils;

public class Publisher {

	private HarvesterOptions options;

	Log log = LogFactory.getLog(Publisher.class);

	/**
	 * Constructor for publisher, If updateList is null a full update will be performed. 
	 */
	public Publisher(HarvesterOptions options){
		this.options = options;
	}

	/**
	 * Publishes files that have been updated to the BaseXDatabase.
	 * @throws BaseXException
	 */
	public void publish() throws Exception{
		if(options.getDb() != null){

			Context context = new Context();

			if(options.getDbPath()!= null){
				context.mprop.setObject("DBPATH", options.getDbPath());
			}

			NesstarUtils utils = new NesstarUtils(options);

			//Open the database
			try{
				new Open(options.getDb()).execute(context);
			}
			catch(BaseXException e){
				new CreateDB(options.getDb()).execute(context);
				new Open(options.getDb()).execute(context);
			}

			File directory = new File(options.getSyncFolder());

			if(directory.exists()){

				//FIXME I need to remove the usage of catalogs and synchronize the folder based on files, 
				//perhaps time stamps in the future if BaseX give them out with the file
				ArrayList<File> files = utils.getFilesFromDirectory(directory);

				//This will hold two lists, an updates list (index 0) and an deletes list (index 1)
				ArrayList<ArrayList<String>> fileLists = compareFolderToDb(files, context);

				if(fileLists.size() != 0){
					ArrayList<String> updates = fileLists.get(0);
					ArrayList<String> deletes = fileLists.get(1);

					//Delete files from database
					System.out.println();
					System.out.println("Checking for files that need to be deleted...");
					if(!deletes.isEmpty()){
						for(String delete : deletes){
							System.out.println("\tDeleting file: "+delete);
							log.info("Deleting file: "+delete);
							try{
								new Delete(delete).execute(context);
							}
							catch(Exception e){
								log.info("Delete failed.");
							}
						}
					}
					else{
						System.out.println("No files to delete.");
						log.info("No files to delete.");
					}
					System.out.println();

					//Incremental Update
					System.out.println("Checking for files that need to be updated...");
					if(!updates.isEmpty()){ 
						for(File file : files){
							for(String name : updates){
								if(file.getName().contains(name) && file.getName().contains(options.getExtensionPrefix()) && !(file.getName().contains(".zip")) && !(file.getName().contains("Catalog.xml"))){
									System.out.println("\tPublishing update: "+file.getName());
									log.info("Publishing update: "+file.getName());
									new Replace(file.getName(),file.getPath()).execute(context);
									break;
								}
							}
						}
					}
					else if(updates.isEmpty()){
						System.out.println("No updates at this point.");
						log.info("No updates at this point.");
					}
					System.out.println();
				}
				else{
					//Database is empty
				}

			}
			else{
				String noCatMessage = "No catalog found in this directory.\n";
				log.info(noCatMessage);
				System.out.println();
				System.out.println(noCatMessage);
			}
			
			System.out.println("Optimizing Database...");
			new Optimize().execute(context);
			System.out.println("Optimization complete.");
			System.out.println();
			//System.out.println(new InfoDB().execute(context));
			new Close().execute(context);	
		}

	}

	private ArrayList<ArrayList<String>> compareFolderToDb(ArrayList<File> files, Context context) throws Exception{

		//Arraylist that will be returned holding both an array for updates and one for deletes
		ArrayList<ArrayList<String>> updatesAndDeletes = new ArrayList<ArrayList<String>>();

		//updates list
		ArrayList<String> updates = new ArrayList<String>();

		//deletes list
		ArrayList<String> deletes = new ArrayList<String>();

		String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";

		String list = new List(options.getDb()).execute(context);
		if(!list.contains("\n0 Resources.")){
			//Get catalog Entries to compare 
			ArrayList<CatalogEntry> newEntries = parseFiles(files);
			ArrayList<CatalogEntry> previousEntries = parseXmlList(list);


			for(CatalogEntry prevEntry : previousEntries){
				String prevId = prevEntry.getId();
				boolean foundId = false;

				for(CatalogEntry curEntry : newEntries){
					if(curEntry.getId().equalsIgnoreCase(prevId)){
						foundId = true;

						if(options.getLastUpdate() != null && !options.getLastUpdate().isEmpty()){
							//turn the current timeStamp into a date 
							SimpleDateFormat sdf = new SimpleDateFormat(format);
							Date dOne = sdf.parse(curEntry.getTimeStamp());

							//turn the previous timeStamp into a date 
							SimpleDateFormat sdf2 = new SimpleDateFormat(format);

							Date dTwo = null;
							dTwo = sdf2.parse(options.getLastUpdate());

							if(dOne.after(dTwo)){
								updates.add(curEntry.getId());
							}
						}
						else{
							updates.add(curEntry.getId());
						}
						break;
					}

				}
				//If the previous entry was not found in the latest catalog it must have been deleted.
				if(!foundId){
					deletes.add(prevEntry.getId());
				}
			}

			for(CatalogEntry curEntry : newEntries){
				String currId = curEntry.getId();
				boolean foundId = false;

				for(CatalogEntry prevEntry : previousEntries){
					if(prevEntry.getId().equalsIgnoreCase(currId)){
						foundId = true;
					}
				}
				if(!foundId){
					updates.add(curEntry.getId());
				}
			}
		}
		//No files found in the DB indicating the database is new
		else{
			log.info("First push into database.");
			ArrayList<CatalogEntry> newEntries = parseFiles(files);

			for(CatalogEntry curEntry : newEntries){
				updates.add(curEntry.getId());
			}
		}

		//	}
		/*	catch(Exception e){
			ArrayList<CatalogEntry> newEntries = parseCatalogXml(catalog);

			for(CatalogEntry curEntry : newEntries){
				updates.add(curEntry.getId());
			}
		}*/

		updatesAndDeletes.add(updates);
		updatesAndDeletes.add(deletes);

		return updatesAndDeletes;
	}

	private ArrayList<CatalogEntry> parseFiles(ArrayList<File> files) throws Exception{

		ArrayList<CatalogEntry> entries = new ArrayList<CatalogEntry>();

		for(File file : files){
			String id = file.getName();
			Date d = new Date(file.lastModified());
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			String timeStamp = dateFormat.format(d);

			CatalogEntry entry = new CatalogEntry(id, timeStamp);
			entries.add(entry);
		}


		return entries;
	}

	private ArrayList<CatalogEntry> parseXmlList(String list) throws Exception{

		ArrayList<CatalogEntry> entries = new ArrayList<CatalogEntry>();

		boolean parseLine = false;

		for(String line : list.split("\n")){

			//We have hit the end of the list
			if(line.isEmpty()){
				break;
			}

			if(parseLine){
				line = line.replaceAll("\\s+", " ");
				String[] fileInfo = line.split("\\s");

				//for now all we can do is look at the file name
				String fileId = fileInfo[0];

				//Do not have time stamps for now. 
				CatalogEntry entry = new CatalogEntry(fileId, "");
				entries.add(entry);
			}

			//We have finished the column headers and are now ready to parse the lines of file info
			if(line.startsWith("-------")){
				parseLine = true;
			}	
		}

		return entries;
	}

	/////////////////////////////////////
	//		GETTERS/SETTERS
	/////////////////////////////////////

	public HarvesterOptions getOptions() {
		return options;
	}

	public void setOptions(HarvesterOptions options) {
		this.options = options;
	}
}
