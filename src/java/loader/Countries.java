package loader;

import utils.Common;

/**
 * Created by fernando on 10/18/16.
 */
public class Countries
{

	private Integer getRecordsNumber()
	{
		//todo: returns number of records of this table in Postgres
		return 0;
	}

	public Boolean truncateTableCountry()
	{
		//todo: remove all records of this table
		return false;
	}

	public Integer populateTableCountry()
	{
		//todo: simple/preparedStatement populate table
		return 0;
	};

	public void run() {
		//todo: log
		//log.info("checking cities");

		//todo: truncateTableCountry();

		//todo: simple/preparedStatement populate table
		//newRecords = populateTableCountry();

		//todo: return how many records created and time elapsed
		//System.out.format("%d records created in %5.2f s. \n", newRecords,Common.elapsedTime()/1000.0 );
	}

}
