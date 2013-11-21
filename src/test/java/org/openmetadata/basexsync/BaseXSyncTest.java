package org.openmetadata.basexsync;

import org.junit.Test;

public class BaseXSyncTest {

	@Test
	public void testPublisher() throws Exception{

		String[] args = {"src/test/resources/basexsync.properties"};

		BaseXSync.main(args);

		//Delete files to create another full update
		//File ddiFolder = new File("/home/andrew/Nesstar/BetaCat");
		//delete(ddiFolder);

		//File database = new File("/home/andrew/BaseXData/BetaCat");
		//delete(database);

	}

}
