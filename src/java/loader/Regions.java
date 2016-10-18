package loader;

import utils.DBConnection;
import org.apache.log4j.Logger;
import utils.Common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by fernando on 10/18/16.
 */
public class Regions
{
	static Logger log = Logger.getLogger(Cities.class.getName());


	private Integer getRecordsNumber()
	{
		Integer count = null;
		try {
			Connection connection = DBConnection.getInstance().getConnection();

			Statement stmt = connection.createStatement();
			String sql = "SELECT count(*) FROM region ";
			ResultSet rs = stmt.executeQuery(sql);

			if (rs.next()) {
				//Retrieve by column index (1-index based)
				count = rs.getInt(1);
			}

		} catch(SQLException e) {
			log.error(e.getMessage());
		}
		return count;
	}

	public Boolean truncateTableRegion()
	{
		Boolean result = null;
		log.info("truncating table regions");

		try {
			Connection connection = DBConnection.getInstance().getConnection();

			Statement stmt = connection.createStatement();
			String sql = "truncate table region ";
			result = stmt.execute(sql);

			if (result) {
				log.debug("truncating table regions successfully, returns true");
			} else {
				log.debug("truncating table regions successfully, returns false");
			}

		} catch(SQLException e) {
			log.error(e.getMessage());
			log.error(e.getCause());
		}

		return result;
	}

	private BufferedReader getBufferedReader() {
		BufferedReader br = null;

		String filename = "./src/resources/fixtures/regions.txt";

		log.info("populate regions with file " + filename);

		try {
			br = new BufferedReader(new FileReader(filename));
		} catch(IOException e) {
			log.error(e.getMessage());
			System.exit(0);
		}
		return br;
	}

	public Integer populateTableRegion()
	{
		BufferedReader br = getBufferedReader();
		Connection connection = DBConnection.getInstance().getConnection();
		Integer count = 0;

		// read first line
		String line;
		boolean skipHeader = true;
		String sql =
			"INSERT INTO public.region (\"RegionId\", \"CountryId\", \"Region\", \"Code\", \"ADM1Code\") VALUES "
				+ "(?, ?, ?, ?, ?)";


		try {
			PreparedStatement pstmt = connection.prepareStatement(sql);
			while ((line = br.readLine())!=null) {
				if(skipHeader) {
					skipHeader = false;
					continue;
				}
				line = line.replace("\"", "");

				String[] fields = line.split(",");
				Integer regionId = Integer.parseInt(fields[0]);
				Integer countryId = Integer.parseInt(fields[1]);
				String region = Common.sanitify(fields[2]);
				String code = fields[3];
				String admCode = fields[4];



				// Set the variables
				pstmt.setInt( 1, regionId );
				pstmt.setInt( 2, countryId );
				pstmt.setString(3, region);
				pstmt.setString( 4, code );
				pstmt.setString(5, admCode);


				log.debug(sql);
				pstmt.executeUpdate();
				count++;
			}
			log.info("populated " + count + " records");

		} catch(IOException e) {
			log.error(e.getMessage());
		}catch(SQLException e){
			log.error(e.getMessage());
		}

		return count;
	}


	public void run() {
		log.info("checking regions");

		Integer count = getRecordsNumber();
		log.info("there are " + count + " regions");

		if (count > 0) {
			truncateTableRegion();
		}

		Integer newRecords;
		Common.startTimer();

		//simple
		newRecords = populateTableRegion();

		Common.endTimer();
		System.out.format("%d region records created in %5.2f s. \n", newRecords,Common.elapsedTime()/1000.0 );
		log.info(Common.elapsedTime() + " ms");
	}

}
