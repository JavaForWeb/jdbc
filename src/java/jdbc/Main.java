package jdbc;

import loader.Countries;
import utils.DBConnection;

import java.sql.Connection;
import java.util.Properties;

import loader.Cities;
import loader.Regions;
import org.apache.log4j.Logger;
import utils.Common;

public class Main {
    static Logger log = Logger.getLogger(Main.class.getName());
    static final String appTitle = "PostgreSQL tutorial with JDBC";

    /*
    public static void PostgreSQLJDBC()
    {
        Connection c = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                .getConnection("jdbc:postgresql://localhost:5432/JavaForWeb", "postgres", "sample");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
        log.info("Opened database successfully");


        try {
            System.out.println("Creating statement...");
            Statement stmt = c.createStatement();
            String sql;
            sql = "SELECT * FROM city";
            ResultSet rs = stmt.executeQuery(sql);

            //STEP 5: Extract data from result set
            while(rs.next()){
                //Retrieve by column name
                int cityId  = rs.getInt("CityId");
                int countryId = rs.getInt("CountryId");
                String city = rs.getString("City");
                String timeZone = rs.getString("TimeZone");

                //Display values
                System.out.print("cityID: " + cityId);
                System.out.print(", countryId: " + countryId);
                System.out.print(", city: " + city);
                System.out.println(", TZ: " + timeZone);
            }



            //STEP 6: Clean-up environment
            rs.close();
            stmt.close();
            c.close();


        } catch(SQLException e ) {
            e.printStackTrace();
        }
    }
    */

    public static void main(String[] args) {
        Common.displayTitle(appTitle);

        Properties config = Common.readConfig();

        log.info("Starting PostgreSQL tutorial ");

        Connection connection = DBConnection.getInstance().getConnection(config);

        new Cities().run();
        new Regions().run();
        new Countries().run();

    }
}
