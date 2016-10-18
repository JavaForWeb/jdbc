package utils;

import loader.Cities;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Created by fernando on 10/17/16.
 */
public class DBConnection
{
	static Logger log = Logger.getLogger(Cities.class.getName());

	private static DBConnection ourInstance = new DBConnection();
	private Connection connection = null;

	public static DBConnection getInstance()
	{
		return ourInstance;
	}

	private DBConnection()
	{
	}

	public Connection getConnection(Properties config)
	{
		String host = config.getProperty("postgresql.host");
		String port = config.getProperty("postgresql.port");
		String database = config.getProperty("postgresql.database");
		String user = config.getProperty("postgresql.user");
		String pass = config.getProperty("postgresql.pass");

		String dsn = "jdbc:postgresql://" + host + ":" + port +"/" + database;
		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection(dsn, user, pass);
		} catch (Exception e) {
			log.error(e.getMessage());
			log.error(e.getCause());
			System.exit(0);
		}
		log.info("database " + database + " successfully opened in " + host + ":" + port);

		return connection;
	}

	public Connection getConnection()
	{
		return connection;
	}

}
