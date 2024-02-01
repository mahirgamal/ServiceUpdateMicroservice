package com.function;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.rabbitmq.client.ConnectionFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.mindrot.jbcrypt.BCrypt;

/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {
    /**
     * This function listens at endpoint "/api/HttpExample". Two ways to invoke it
     * using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/HttpExample
     * 2. curl "{your host}/api/HttpExample?name=HTTP%20Query"
     */

    private static final Logger logger = Logger.getLogger(Function.class.getName());
    private static final String DB_URL = "jdbc:mysql://leisadb.mysql.database.azure.com:3306/leisa";
    private static final String DB_USER = "lei";
    private static final String DB_PASSWORD = "mahirgamal123#";

    private String username, password;

    static {
        try {
            // Load the JDBC driver
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @FunctionName("updateService")
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", methods = {
                    HttpMethod.PUT }, route = "update/{id}", authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<User>> request,
            @BindingName("id") Long id,
            final ExecutionContext context) throws SQLException {
        logger.info("Received request to update user with ID: " + id);

        User updatedUser = request.getBody().orElse(null);
        if (updatedUser == null) {
            logger.warning("Invalid user data in request body.");
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Invalid user data").build();
        }

        if (isAuthorized(request, id) && !isUserInformationExists(updatedUser, id)) {
            boolean updateResult = updateUser(id, updatedUser);
            if (updateResult) {
                logger.info("User with ID " + id + " updated successfully.");
                return request.createResponseBuilder(HttpStatus.OK).body("Updated").build();
            } else {
                logger.warning("Failed to update user with ID " + id);
                return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Error updating user")
                        .build();
            }
        } else {
            logger.warning("Authorization failed or user information exists for user with ID " + id);
            return request.createResponseBuilder(HttpStatus.UNAUTHORIZED).body("Unauthorized").build();
        }
    }

    private boolean isAuthorized(HttpRequestMessage<Optional<User>> request, Long id) {
        // Parse the Authorization header
        final String authHeader = request.getHeaders().get("authorization");
        if (authHeader == null || !authHeader.startsWith("Basic ")) {
            return false;
        }

        // Extract and decode username and password
        String base64Credentials = authHeader.substring("Basic ".length()).trim();
        byte[] credDecoded = Base64.getDecoder().decode(base64Credentials);
        String credentials = new String(credDecoded);

        // credentials = username:password
        final String[] values = credentials.split(":", 2);

        if (values.length != 2) {
            return false; // Incorrect format of the header
        }

        username = values[0];
        password = values[1];

        String sql = "SELECT * FROM users WHERE id=?";

        try (java.sql.Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setLong(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    // Compare the provided password with the hashed password in the database
                    if (BCrypt.checkpw(password, rs.getString("password")) && username.equals(rs.getString("username")))
                        return true;
                    else

                        return false;
                } else
                    return false;
            }
        } catch (SQLException e) {
            // Handle exceptions (log it or throw as needed)
            e.printStackTrace();
        }

        return false;

    }

    private boolean isUserInformationExists(User user, Long userId) throws SQLException {
        String sql = "SELECT COUNT(*) FROM users WHERE  email = ? AND id <> ?";

        try (java.sql.Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, user.getEmail());
            stmt.setLong(2, userId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int count = rs.getInt(1);
                    return count > 0;
                }
            }
        }
        return false;
    }

    private boolean updateUser(Long id, User updatedUser) throws SQLException {
        logger.info("Updating user in database and RabbitMQ with ID: " + id);
        if (rabbitmqUpdateUser(username, updatedUser.getPassword())) {
            String sql = "UPDATE users SET email = ?, password = ? WHERE id = ?";

            try (java.sql.Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                    PreparedStatement stmt = conn.prepareStatement(sql)) {
                        String hashedPassword = BCrypt.hashpw(updatedUser.getPassword(), BCrypt.gensalt());


                stmt.setString(1, updatedUser.getEmail());
                stmt.setString(2, hashedPassword);
                stmt.setLong(3, id);

                stmt.executeUpdate();
                logger.info("Database and RabbitMQ update successful for user ID: " + id);

                return true;
            }
        } else {
            logger.warning("Failed to update RabbitMQ for user ID: " + id);
            return false;
        }
    }

    public boolean rabbitmqUpdateUser(String brokerUsername, String newBrokerPassword) {
        try {
            // Read the JSON file (e.g., 'config.json')
            // You'll need to use a library like Jackson or Gson for JSON parsing in Java
            // Here, we assume that you have a Config class to represent the configuration
            // object

            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("rabbitmqconfig.json");
            if (inputStream == null) {
                throw new IOException("rabbitmqconfig.json file not found in resources");
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            String configJSON = reader.lines().collect(Collectors.joining("\n"));
            JSONObject config = new JSONObject(configJSON);

            // Configuration values
            String brokerType = config.getString("brokerType");
            String brokerProtocol = config.getString("brokerProtocol");
            String brokerHost = config.getString("brokerHost");
            int brokerPort = config.getInt("brokerPort");
            String username = config.getString("brokerUsername");
            String password = config.getString("brokerPassword");

            logger.info("Broker Type: " + brokerType);
            logger.info("Broker Protocol: " + brokerProtocol);
            logger.info("Broker Host: " + brokerHost);
            logger.info("Broker Port: " + brokerPort);
            logger.info("Username: " + username);
            logger.info("Password: " + password);

            // Create a connection to RabbitMQ
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(brokerHost);
            factory.setPort(brokerPort);
            factory.setUsername(username);
            factory.setPassword(password);

            String brokerApiBaseUrl = config.getString("apiUrl");

            logger.info("brokerApiBaseUrl: " + brokerApiBaseUrl);

            String auth = username + ":" + password;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());


            String userJson = "{\"password\":\"" + newBrokerPassword + "\",\"tags\":\"management\"}";

            HttpClient client = HttpClient.newBuilder()
                    .build();

            HttpRequest updateUserRequest = HttpRequest.newBuilder()
                    .uri(URI.create(brokerApiBaseUrl + "/users/" + brokerUsername))
                    .timeout(Duration.ofMinutes(1))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Basic " + encodedAuth)
                    .PUT(HttpRequest.BodyPublishers.ofString(userJson))
                    .build();

            HttpResponse<String> updateUserResponse = client.send(updateUserRequest,
                    HttpResponse.BodyHandlers.ofString());

            logger.info("Update user response status: " + updateUserResponse.statusCode());
            logger.info("Update user response body: " + updateUserResponse.body());

            if (updateUserResponse.statusCode() == 200 || updateUserResponse.statusCode() == 204) {
                logger.info("User updated successfully");
                return true;
            } else {
                logger.warning("Failed to update user. Status code: " + updateUserResponse.statusCode());
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.warning("Failed to update user. Error: " + e.getMessage());
        }
        return false;
    }

}
