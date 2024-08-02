package com.function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Logger;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

/**
 * Azure Functions with Timer trigger.
 */
public class TimerTriggerJava1 {
    private List<Integer> Temperature;
    private List<Integer> Wind_Speed;
    private List<Integer> Humidity;
    private List<Integer> CO2;

    public TimerTriggerJava1(){
        // initialise lists
        Temperature = new ArrayList<>();
        Wind_Speed = new ArrayList<>();
        Humidity = new ArrayList<>();
        CO2 = new ArrayList<>();
    }

    // Get methods
    public List<Integer> getTemperature() {
        return Temperature;
    }

    public List<Integer> getWind_Speed() {
        return Wind_Speed;
    }

    public List<Integer> getHumidity() {
        return Humidity;
    }

    public List<Integer> getCO2() {
        return CO2;
    }

    //function to connect to a databse
    public static Connection connect_DB() throws Exception{
        //seeting up logger
        Logger logger = Logger.getLogger(Function.class.getName());
        logger.info("Connecting to the database");
        String url="jdbc:sqlserver://sc21rh.database.windows.net:1433;database=sc21rh;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;";
        // connecting to database with url, username and password 
        Connection connection = DriverManager.getConnection(url, "", "");
        logger.info("Connected to the database successfully");
        //logger.info(connection.getCatalog());

        // check if schema already exists
        int rowCount = get_rowCount(connection);
        // if rowcount == 0 no db OR >200 we reset schema
        if(rowCount==0 || rowCount == 200){
            if(rowCount == 200){
                logger.info("Resetting database");
            }
            else{
                logger.info("Creating database schema");
            }           
            //get sql command to create DB schema
            Scanner scanner = new Scanner(Function.class.getClassLoader().getResourceAsStream("db.sql"));

            Statement statement = connection.createStatement();
            // read all lines of the sql command
            while (scanner.hasNextLine()) {
                // execute command
                String query = scanner.nextLine();
                statement.execute(query);
            }
            logger.info("Created database schema successfully");
            scanner.close();
        }
        return connection;      
    }


    /**
     * This function will be invoked periodically according to the specified schedule.
     * @throws Exception
     */
    @FunctionName("TimerTriggerJava1")
    public void run(
        @TimerTrigger(name = "timerInfo", schedule = "0 */1 * * * *") String timerInfo,
        final ExecutionContext context
        
    ) throws Exception {
        context.getLogger().info("Java Timer trigger function executed at: " + LocalDateTime.now());

        // Create Database connection
        Connection connection = connect_DB();
        Logger logger = context.getLogger();

        // generating 20 weather probes of data and writing to database
        int i;
        for(i = 0;i<20;i++){
            generate_data(i);

            // get number of rows in databse
            int rowCount = get_rowCount(connection);
            
            // write to database
            String insertQuery = "INSERT INTO weather (id,sensor_id, temperature , windspeed , humidity , co2) VALUES (?,?,?,?,?,?);";
            PreparedStatement statement = connection.prepareStatement(insertQuery);
            logger.info(String.format(" ADDED ID: %d, SensorID %d data: %d %d %d %d",rowCount,i,Temperature.get(i),Wind_Speed.get(i),Humidity.get(i),CO2.get(i)));

            statement.setInt(1, rowCount);
            statement.setInt(2, i);
            statement.setInt(3, Temperature.get(i));
            statement.setInt(4, Wind_Speed.get(i));
            statement.setInt(5, Humidity.get(i));
            statement.setInt(6, CO2.get(i));

            // Insert into table
            statement.executeUpdate();
            //calculate minimums
            calculate_min(connection,"temperature",i);
            calculate_min(connection,"windspeed",i);
            calculate_min(connection,"humidity",i);
            calculate_min(connection,"co2",i);

            //calculate maximums
            calculate_max(connection,"temperature",i);
            calculate_max(connection,"windspeed",i);
            calculate_max(connection,"humidity",i);
            calculate_max(connection,"co2",i);

            //calculate means
            calculate_mean(connection,"temperature",i);
            calculate_mean(connection,"windspeed",i);
            calculate_mean(connection,"humidity",i);
            calculate_mean(connection,"co2",i);
        }         

        // check if database has updates
        // calculate statistical functions
        
    }
    // funtion to generate random weather data
    public int generate_data(int ID){
        
        // generate random data
        int random_temperature = (int)Math.floor(Math.random() * (15 - 8 + 1) + 8);
        int random_windspeed = (int)Math.floor(Math.random() * (25 - 15 + 1) + 15);
        int random_humidity = (int)Math.floor(Math.random() * (70 - 40 + 1) + 40);
        int random_co2 = (int)Math.floor(Math.random() * (1500 - 500 + 1) + 500);

        // add data to list
        Temperature.add(random_temperature);
        Wind_Speed.add(random_windspeed);
        Humidity.add(random_humidity);
        CO2.add(random_co2);

        Logger logger = Logger.getLogger(Function.class.getName());
        logger.info(String.format("Added ID %d data: %d %d %d %d",ID,random_temperature,random_windspeed,random_humidity,random_co2));

        return 0;
    }

    public static int get_rowCount(Connection connection) throws SQLException{
            // prepare and execute select command
            String getRows ="SELECT COUNT(*) AS row_count FROM weather";
            PreparedStatement preparedStatement = connection.prepareStatement(getRows);
            ResultSet resultSet = preparedStatement.executeQuery();
            // Check if the resultset has data
            int rowCount = 0;
            if (resultSet.next()) {
                // get rowCount
                rowCount = resultSet.getInt("row_count");
            }
        //return
        return rowCount;
    }

    //  function to output minimum data piece
    public Double calculate_min(Connection connection, String target, int ID) throws SQLException{
        
        // get all data from the database where weather.id = id
        String selectStatement = "SELECT "+target;
        PreparedStatement readStatement = connection.prepareStatement(selectStatement+" FROM weather WHERE weather.sensor_id = "+ID+";");
        ResultSet resultSet = readStatement.executeQuery();
        
        double minimum = 100000;
        // read all returned row values
        while(resultSet.next()){            
            int value = resultSet.getInt(target);
            // compare and chose minimum of the two
            if(value<minimum){
                minimum=value;
            }
        }

        //logging  
        Logger logger = Logger.getLogger(Function.class.getName());      
        logger.info(String.format("Added Minimum %s for sensor ID:%d. Minimum = %.1f",target,ID,(double)minimum));

        return minimum;
    }

    //  function to output maximum data piece
    public Double calculate_max(Connection connection, String target, int ID) throws SQLException{
        
        // get all data from the database where weather.id = id
        String selectStatement = "SELECT "+target;
        PreparedStatement readStatement = connection.prepareStatement(selectStatement+" FROM weather WHERE weather.sensor_id = "+ID+";");
        ResultSet resultSet = readStatement.executeQuery();
        
        double maximum = 0;
        // read all returned row values
        while(resultSet.next()){
            // compare two
            int value = resultSet.getInt(target);
            if(value>maximum){
                maximum=value;
            }
        }

        //logging   
        Logger logger = Logger.getLogger(Function.class.getName());     
        logger.info(String.format("Added Maximum %s for sensor ID:%d. Maximum = %.1f",target,ID,(double)maximum));

        return maximum;
    }

    //  function to output mean data 
    public Double calculate_mean(Connection connection, String target, int ID) throws SQLException{
        Logger logger = Logger.getLogger(Function.class.getName());
        Double mean = 0.0;
        // get all values for target column where weather.id = id
        String selectStatement = "SELECT "+target;
        PreparedStatement readStatement = connection.prepareStatement(selectStatement+" FROM weather WHERE weather.sensor_id = "+ID+";");
        ResultSet resultSet = readStatement.executeQuery();
        //calculate the sum and no. of element
        int sum = 0;int count = 0;
        while(resultSet.next()){
            //get next value, add to sum and increment sum
            sum = sum + resultSet.getInt(target);
            count = count+1;
        }
        //calculate mean
        mean = (double) sum/count;
        logger.info(String.format("Added Mean %s for sensor ID:%d. Mean = %.1f",target,ID,mean));

        return mean;
    }

    
}
