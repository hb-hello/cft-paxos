package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ServerManager {

    private static final Logger logger = LogManager.getLogger(ServerManager.class);
    private static final List<Process> processes = new ArrayList<>();

    public static void startAllServerProcesses() {
        logger.info("Starting all server node processes...");

        Map<String, ServerDetails> servers = Config.getServers();

        for (String serverId : servers.keySet()) {
            try {
                startServerProcess(serverId);
                logger.info("Started server process for: {}", serverId);
            } catch (IOException e) {
                logger.error("Failed to start server process for {}: {}", serverId, e.getMessage());
                throw new RuntimeException("Failed to start server: " + serverId, e);
            }
        }

        // Give servers time to start up
        logger.info("Waiting for servers to initialize...");
        try {
            Thread.sleep(3000); // Wait 3 seconds for all servers to start
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for servers to start");
            Thread.currentThread().interrupt();
        }
    }

    private static void startServerProcess(String serverId) throws IOException {
        // Get the java executable path
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + "/bin/java";

        // Get the classpath
        String classpath = System.getProperty("java.class.path");

        ProcessBuilder processBuilder = getProcessBuilder(serverId, javaBin, classpath);

        // Store the process reference in serverNodes map for later management
        // serverNodes.get(serverId).setProcess(process);
        Process process = processBuilder.start();
        processes.add(process);
    }

    private static ProcessBuilder getProcessBuilder(String serverId, String javaBin, String classpath) {
        // Build the Java command to run the server
//        String javaCommand = String.format("\"%s\" -cp \"%s\" org.example.ServerNode %s", javaBin, classpath, serverId);

        String javaCommand = "java -jar C:\\Users\\hbiyani\\OneDrive - Stony Brook University\\Documents\\DEV\\cft-hb-hello\\stable-paxos\\server\\target\\server-1.0-SNAPSHOT-jar-with-dependencies.jar";

        List<String> commands = new ArrayList<>();
        commands.add("cmd.exe");
        commands.add("start"); // Opens a new CMD window
        commands.add("cmd.exe");
        //commands.add("\\\"Server \" + serverId + \"\\\""); // Specifies that 'start' should open a new CMD window
        commands.add("/K"); // Keeps the *new* CMD window open after the command executes
        commands.add(javaCommand);
        System.out.println(commands);

        // Build the command
        return new ProcessBuilder(commands);
    }

    // Add shutdown hook to clean up processes
    public static void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down all server processes...");
            for (Process process : processes) {
                if (process != null && process.isAlive()) {
                    process.destroy();
                    try {
                        // Wait up to 5 seconds for graceful shutdown
                        if (!process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                            process.destroyForcibly();
                        }
                    } catch (InterruptedException e) {
                        logger.error("Interrupted while waiting for server shutdown");
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }));
    }

}
