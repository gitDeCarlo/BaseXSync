package org.openmetadata.basexsync;

import org.basex.core.Context;
import org.basex.core.cmd.Close;
import org.basex.core.cmd.CreateDB;
import org.basex.core.cmd.List;
import org.basex.core.cmd.Open;
import org.junit.Test;

public class BaseXSyncTest {

	@Test
	public void testPublisher() throws Exception{

		String[] args = {"src/test/resources/basexsync.properties"};

		BaseXSync.main(args);

	}
	
	@Test
	public void testBaseXList() throws Exception{
		
		Context context = new Context();

		context.mprop.setObject("DBPATH", "/home/andrew/BaseXData/");
			
		new Open("ch_fors_compass").execute(context);
		String list = new List("ch_fors_compass").execute(context);
		//new CreateDB("ch_forsd_compass").execute(context);
		//String list = new List("ch_forsd_compass").execute(context);
		for(String line : list.split("\n")){
			System.out.println(line.replaceAll("\\s+", " "));
		}
		System.out.println(list.contains("\n0 Resources."));
		
		new Close().execute(context);
	}

}
