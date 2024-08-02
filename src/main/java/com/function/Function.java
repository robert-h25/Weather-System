package com.function;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.OutputBinding;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.microsoft.azure.functions.annotation.QueueOutput;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.logging.Logger;

/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {
    private List<Integer> Temperature;
    private List<Integer> Wind_Speed;
    private List<Integer> Humidity;
    private List<Integer> CO2;

    public Function(){
        // initialise lists
        Temperature = new ArrayList<>();
        Wind_Speed = new ArrayList<>();
        Humidity = new ArrayList<>();
        CO2 = new ArrayList<>();
    }

    // Get list methods
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
        Logger logger = Logger.getLogger(Function.class.getName());
        logger.info("Connecting to the database");
        String url="jdbc:sqlserver://sc21rh.database.windows.net:1433;database=sc21rh;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;";
        Connection connection = DriverManager.getConnection(url, "", "");
        logger.info("Connected to the database successfully");
        //logger.info(connection.getCatalog());
        logger.info("Creating database schema");
        Scanner scanner = new Scanner(Function.class.getClassLoader().getResourceAsStream("db.sql"));
        
        Statement statement = connection.createStatement();
        while (scanner.hasNextLine()) {
            String query = scanner.nextLine();
            statement.execute(query);
        }
        logger.info("Created database schema successfully");
        scanner.close();

        return connection;
        
    }

    /**
     * This function listens at endpoint "/api/HttpExample". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/HttpExample
     * 2. curl "{your host}/api/HttpExample?name=HTTP%20Query"
     * @throws Exception
     */
    @FunctionName("HttpExample")
    public HttpResponseMessage run(
            @HttpTrigger(
                name = "req",
                methods = {HttpMethod.GET, HttpMethod.POST},
                authLevel = AuthorizationLevel.ANONYMOUS)
                HttpRequestMessage<Optional<String>> request,
                @QueueOutput(name = "msg", queueName = "outqueue", 
            connection = "AzureWebJobsStorage") OutputBinding<String> msg,
            final ExecutionContext context
            ) throws Exception {
        context.getLogger().info("Java HTTP trigger processed a request.");
        
        //allowing for use of timetrigger
        String query = request.getQueryParameters().get("name");
        String name = request.getBody().orElse(query);
        msg.setValue(name);


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
            logger.info(String.format("ID: %d ,SensorID %d data: %d %d %d %d",rowCount,i,Temperature.get(i),Wind_Speed.get(i),Humidity.get(i),CO2.get(i)));

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

        return request.createResponseBuilder(HttpStatus.OK).body("Leeds Weather Data").build();
        
    }
    // funtion to generate random weather data
    public int generate_data(int ID){
        
        // generate data
        int random_temperature = (int)Math.floor(Math.random() * (15 - 8 + 1) + 8);
        int random_windspeed = (int)Math.floor(Math.random() * (25 - 15 + 1) + 15);
        int random_humidity = (int)Math.floor(Math.random() * (70 - 40 + 1) + 40);
        int random_co2 = (int)Math.floor(Math.random() * (1500 - 500 + 1) + 500);

        // add data to list
        Temperature.add(random_temperature);
        Wind_Speed.add(random_windspeed);
        Humidity.add(random_humidity);
        CO2.add(random_co2);

        //Logger logger = Logger.getLogger(Function.class.getName());
        //logger.info(String.format("Added ID %d data: %d %d %d %d",ID,random_temperature,random_windspeed,random_humidity,random_co2));

        return 0;
    }

    public int get_rowCount(Connection connection) throws SQLException{
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
        //initialising sum and count variables
        double minimum = 100000;
        while(resultSet.next()){
            //get next value, ad to sum and increment sum
            int value = resultSet.getInt(target);
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
        //initialising sum and count variables
        double maximum = 0;
        while(resultSet.next()){
            //get next value, ad to sum and increment sum
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
            //get next value, ad to sum and increment sum
            sum = sum + resultSet.getInt(target);
            count = count+1;
        }
        //calculate mean
        mean = (double) sum/count;
        logger.info(String.format("Added Mean %s for sensor ID:%d. Mean = %.1f",target,ID,mean));

        return mean;
    }
}
