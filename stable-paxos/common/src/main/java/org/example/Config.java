package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class Config {

    private static final Logger logger = LogManager.getLogger(Config.class);

    // Static configuration fields - accessible from anywhere
    private static Map<String, ServerDetails> servers;
    private static Map<String, Double> clientBalances;
    private static String transactionSetsPath;
    private static long clientTimeoutMillis;
    private static int maxRetries;
    private static boolean initialized = false;

    // Private constructor to prevent instantiation
    private Config() {
        throw new UnsupportedOperationException("Config is a utility class and cannot be instantiated");
    }

    /**
     * Initialize configuration using custom file paths
     */
    public static synchronized void initialize() {
        if (initialized) {
            logger.warn("Config already initialized. Reinitializing...");
        }

        // Load properties
        Properties props = loadProperties("config.properties");

        String serverDetailsPath = props.getProperty(
                "server.details.path",
                "src/main/resources/serverDetails.json"
        );

        String clientDetailsPath = props.getProperty(
                "client.details.path",
                "src/main/resources/clientDetails.json"
        );

        transactionSetsPath = props.getProperty(
                "transactions.sets.path",
                "src/main/resources/transactionSets.csv"
        );

        clientTimeoutMillis = Long.parseLong(props.getProperty(
                "client.timeout.millis",
                "500"
        ));

        maxRetries = Integer.parseInt(props.getProperty(
                "max.retries",
                "src/main/resources/transactionSets.csv"
        ));

        logger.info("Using paths: server.details.path={}, client.details.path={}",
                serverDetailsPath, clientDetailsPath);

        logger.info("Loading server details from: {}", serverDetailsPath);
        servers = ConfigLoader.loadServersFromConfig(serverDetailsPath);
        logger.info("Loaded {} servers", servers.size());

        logger.info("Loading client details from: {}", clientDetailsPath);
        clientBalances = ConfigLoader.loadClientDetails(clientDetailsPath);
        logger.info("Loaded {} clients", clientBalances.size());

        initialized = true;
        logger.info("Config initialization complete");
    }

    /**
     * Load properties from file
     */
    private static Properties loadProperties(String filePath) {
        Properties props = new Properties();

        // Try loading from file system first
        try (InputStream input = new FileInputStream(filePath)) {
            props.load(input);
            logger.info("Successfully loaded properties file from: {}", filePath);
        } catch (IOException e) {
            logger.warn("Could not load properties file from {}, trying classpath: {}", filePath, e.getMessage());

            // Try loading from classpath as fallback
            try (InputStream input = Config.class.getClassLoader().getResourceAsStream(filePath)) {
                if (input != null) {
                    props.load(input);
                    logger.info("Successfully loaded properties file from classpath: {}", filePath);
                } else {
                    logger.warn("Properties file not found in classpath, using default values");
                }
            } catch (IOException ex) {
                logger.warn("Could not load properties file from classpath, using default values: {}", ex.getMessage());
            }
        }

        return props;
    }

    /**
     * Check if configuration has been initialized
     */
    public static boolean isInitialized() {
        return initialized;
    }

    /**
     * Ensure configuration is initialized before access
     */
    private static void ensureInitialized() {
        if (!initialized) {
            throw new IllegalStateException("Config has not been initialized. Call Config.initialize() first.");
        }
    }

    /**
     * Get all server details
     */
    public static Map<String, ServerDetails> getServers() {
        ensureInitialized();
        return servers;
    }

    /**
     * Get all server IDs
     */
    public static Set<String> getServerIds() {
        ensureInitialized();
        return servers.keySet();
    }

    /**
     * Get a specific server by ID
     */
    public static ServerDetails getServer(String serverId) {
        ensureInitialized();
        return servers.get(serverId);
    }

    public static int getServerPort(String serverId) {
        ensureInitialized();
        if (servers.containsKey(serverId)) {
            return servers.get(serverId).port();
        } else {
            throw new NoSuchElementException(("Server ID " + serverId + " not found in config"));
        }
    }

    /**
     * Get all client balances
     */
    public static Map<String, Double> getClientBalances() {
        ensureInitialized();
        return clientBalances;
    }

    /**
     * Get a specific client's starting balance
     */
    public static Double getClientBalance(String clientId) {
        ensureInitialized();
        return clientBalances.get(clientId);
    }


    public static String getTransactionSetsPath() {
        ensureInitialized();
        return transactionSetsPath;
    }

    public static int getMaxRetries() {
        ensureInitialized();
        return maxRetries;
    }

    public static long getClientTimeoutMillis() {
        ensureInitialized();
        return clientTimeoutMillis;
    }

    /**
     * Check if a server exists in the configuration
     */
    public static boolean hasServer(String serverId) {
        ensureInitialized();
        return servers.containsKey(serverId);
    }

    /**
     * Check if a client exists in the configuration
     */
    public static boolean hasClient(String clientId) {
        ensureInitialized();
        return clientBalances.containsKey(clientId);
    }
}