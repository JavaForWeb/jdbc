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
import java.util.ArrayList;

/**
 * Created by fernando on 10/17/16.
 */
public class Cities
{
	static Logger log = Logger.getLogger(Cities.class.getName());


	public class MyThread extends Thread
	{
		String line;
		Connection connection;
		PreparedStatement preparedStatement;

		public void run() {
			line = line.replace("\"", "");
			try {
				String[] fields = line.split(",");
				Integer cityId = Integer.parseInt(fields[0]);
				Integer countryId = Integer.parseInt(fields[1]);
				Integer regionId = Integer.parseInt(fields[2]);
				String city = Common.sanitify(fields[3]);
				Double latitude = Double.parseDouble(fields[4]);
				Double longitude = Double.parseDouble(fields[5]);
				String timezone = fields[6];
				Integer dmaId = Integer.parseInt(fields[7]);
				String code = fields[8];

				Statement stmt = connection.createStatement();
				String sql =
					"INSERT INTO public.\"city\" (\"CityId\", \"CountryId\", \"RegionId\", \"City\", \"Latitude\", \"Longitude\", \"TimeZone\", \"DmaId\", \"CODE\") VALUES\n"
						+ "(" +
						cityId + "," +
						countryId + "," +
						regionId + "," +
						"'" + city + "'," +
						latitude + "," +
						longitude + "," +
						"'" + timezone + "'," +
						dmaId + "," +
						"'" + code + "' );";
				log.debug(sql);
				stmt.execute(sql);

			} catch(SQLException e) {
				log.error(e.getMessage());
			}
		}

		public void setPayload (String payload) {
			line = payload;
		}

		public void setConnection(Connection conn) {
			connection = conn;
		}

		public void setPreparedStatement(PreparedStatement pstmt) {
			preparedStatement = pstmt;
		}
	}
	private Integer getRecordsNumber()
	{
		Integer count = null;
		try {
			Connection connection = DBConnection.getInstance().getConnection();

			Statement stmt = connection.createStatement();
			String sql = "SELECT count(*) FROM city ";
			ResultSet rs = stmt.executeQuery(sql);

			if (rs.next()) {
				//Retrieve by column index (1-index based)
				count = rs.getInt(1);
			}

		} catch(SQLException e) {
			log.error(e.getMessage());
			log.error(e.getCause());
		}
		return count;
	}

	public Boolean truncateTableCity()
	{
		Boolean result = null;
		log.info("truncating table cities");

		try {
			Connection connection = DBConnection.getInstance().getConnection();

			Statement stmt = connection.createStatement();
			String sql = "truncate table city ";
			result = stmt.execute(sql);

			if (result) {
				log.debug("truncating table cities successfully, returns true");
			} else {
				log.debug("truncating table cities successfully, returns false");
			}

		} catch(SQLException e) {
			log.error(e.getMessage());
			log.error(e.getCause());
		}

		return result;
	}

	private BufferedReader getBufferedReader() {
		BufferedReader br = null;

		String filename = "./src/resources/fixtures/cities.txt";

		log.info("populate cities with file " + filename);

		try {
			br = new BufferedReader(new FileReader(filename));
		} catch(IOException e) {
			log.error(e.getMessage());
			System.exit(0);
		}
		return br;
	}

	public Integer populateTableCityPrepareStatement()
	{
		BufferedReader br = getBufferedReader();
		Connection connection = DBConnection.getInstance().getConnection();
		Integer count = 0;

		// read first line
		String line;
		boolean skipHeader = true;
		String sql =
			"INSERT INTO public.city (\"CityId\", \"CountryId\", \"RegionId\", \"City\", \"Latitude\", \"Longitude\", \"TimeZone\", \"DmaId\", \"CODE\") VALUES "
				+ "(?, ?, ?, ?, ?, ?, ?, ?, ?)";


		try {
			PreparedStatement pstmt = connection.prepareStatement(sql);
			while ((line = br.readLine())!=null) {
				if(skipHeader) {
					skipHeader = false;
					continue;
				}
				line = line.replace("\"", "");

				String[] fields = line.split(",");
				Integer cityId = Integer.parseInt(fields[0]);
				Integer countryId = Integer.parseInt(fields[1]);
				Integer regionId = Integer.parseInt(fields[2]);
				String city = Common.sanitify(fields[3]);
				Double latitude = Double.parseDouble(fields[4]);
				Double longitude = Double.parseDouble(fields[5]);
				String timezone = fields[6];
				Integer dmaId = Integer.parseInt(fields[7]);
				String code = fields[8];


				// Set the variables
				pstmt.setInt( 1, cityId );
				pstmt.setInt( 2, countryId );
				pstmt.setInt( 3, regionId );
				pstmt.setString( 4, city );
				pstmt.setDouble(5, latitude);
				pstmt.setDouble(6, longitude );
				pstmt.setString(7, timezone );
				pstmt.setInt( 8, dmaId );
				pstmt.setString(9, code );

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

	public Integer populateTableCity()
	{
		BufferedReader br = getBufferedReader();
		Connection connection = DBConnection.getInstance().getConnection();
		Integer count = 0;

		// read first line
		String line;
		boolean skipHeader = true;
		try {
			while ((line = br.readLine())!=null) {
				if(skipHeader) {
					skipHeader = false;
					continue;
				}
				line = line.replace("\"", "");

				String[] fields = line.split(",");
				Integer cityId = Integer.parseInt(fields[0]);
				Integer countryId = Integer.parseInt(fields[1]);
				Integer regionId = Integer.parseInt(fields[2]);
				String city = Common.sanitify(fields[3]);
				Double latitude = Double.parseDouble(fields[4]);
				Double longitude = Double.parseDouble(fields[5]);
				String timezone = fields[6];
				Integer dmaId = Integer.parseInt(fields[7]);
				String code = fields[8];

				Statement stmt = connection.createStatement();
				String sql =
					"INSERT INTO public.\"city\" (\"CityId\", \"CountryId\", \"RegionId\", \"City\", \"Latitude\", \"Longitude\", \"TimeZone\", \"DmaId\", \"CODE\") VALUES\n"
						+ "(" +
						cityId + "," +
						countryId + "," +
						regionId + "," +
						"'" + city + "'," +
						latitude + "," +
						longitude + "," +
						"'" + timezone + "'," +
						dmaId + "," +
						"'" + code + "' );";
				log.debug(sql);
				stmt.execute(sql);
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

	public Integer threadPopulateTableCity()
	{

		BufferedReader br = getBufferedReader();
		Connection connection = DBConnection.getInstance().getConnection();
		Integer count = 0;
		Integer threadsCount = 3;

		ArrayList<PreparedStatement> arrayPrepStatement = new ArrayList<>();
		ArrayList<MyThread> arrayThreads = new ArrayList<>(threadsCount);

		String sql =
			"INSERT INTO public.city (\"CityId\", \"CountryId\", \"RegionId\", \"City\", \"Latitude\", \"Longitude\", \"TimeZone\", \"DmaId\", \"CODE\") VALUES "
				+ "(?, ?, ?, ?, ?, ?, ?, ?, ?)";

		String line;
		boolean skipHeader = true;
		try {

			for (int i=0; i <threadsCount; i++) {
				PreparedStatement pstmt = connection.prepareStatement(sql);
				arrayPrepStatement.add(pstmt);
				arrayThreads.add((MyThread)null);
			}

			while ((line = br.readLine())!=null) {
				// skip first line
				if(skipHeader) {
					skipHeader = false;
					continue;
				}
				int threadsCreated = 0;
				for (int i=0; i < threadsCount; i++) {
					MyThread myThread = new MyThread();
					myThread.setConnection(connection);
					//myThread.setPreparedStatement(arrayPrepStatement.get(i));
					myThread.setPreparedStatement(arrayPrepStatement.get(0));
					myThread.setPayload(line);
					arrayThreads.set(i, myThread);
					myThread.run();
					threadsCreated++;
					if (i < threadsCount -1 && (line = br.readLine())==null) {
						break;
					}
				}

				for (int i=0; i < threadsCreated; i++) {
					if (arrayThreads.get(i).isAlive()) {
						try {
							arrayThreads.get(i).join();
						} catch(InterruptedException e) {
							log.error(e.getMessage());
						}
					}
				}

				count+=threadsCreated;
			}
			log.info("populated " + count + " records");

		} catch(IOException e) {
			log.error(e.getMessage());
		} catch(SQLException e) {
			log.error(e.getMessage());
		}

		return count;
	}

	public void run() {
		log.info("checking cities");

		Integer count = getRecordsNumber();
		log.info("there are " + count + " cities");

		if (count > 0) {
			truncateTableCity();
		}

		Integer newRecords;
		Common.startTimer();

		//simple
		newRecords = populateTableCity();

		//with PrepareStatement
		//newRecords = populateTableCityPrepareStatement();

		//using threads
		//newRecords = threadPopulateTableCity();

		Common.endTimer();
		System.out.format("%d city records created in %5.2f s. \n", newRecords,Common.elapsedTime()/1000.0 );
		log.info(Common.elapsedTime() + " ms");
	}

}
