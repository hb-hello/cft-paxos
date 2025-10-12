@echo off
TITLE Paxos System Launcher

ECHO #######################################################
ECHO #         Multi-Paxos Banking System Launcher         #
ECHO #######################################################
ECHO.
ECHO This script will start 5 server nodes and 1 client CLI.
ECHO Make sure server.jar and cli.jar are in the same directory.
ECHO.

:: Check for Java installation
java -version >nul 2>&1
if %errorlevel% neq 0 (
    ECHO ERROR: Java is not installed or not found in your system's PATH.
    ECHO Please install Java and try again.
    PAUSE
    exit /b
)

ECHO Starting 5 server nodes in separate terminal windows...
ECHO.

:: The "Title" for the window is the first quoted argument to START.
:: The command to run is `cmd /k` which keeps the window open after the command finishes.
:: The command itself is `java -jar server.jar <server-id>`.
:: The server IDs "s1", "s2", etc., are the hardcoded string arguments.
start "Server 1" cmd /k "java -jar server-1.0-SNAPSHOT-jar-with-dependencies.jar n1"
start "Server 2" cmd /k "java -jar server-1.0-SNAPSHOT-jar-with-dependencies.jar n2"
start "Server 3" cmd /k "java -jar server-1.0-SNAPSHOT-jar-with-dependencies.jar n3"
start "Server 4" cmd /k "java -jar server-1.0-SNAPSHOT-jar-with-dependencies.jar n4"
start "Server 5" cmd /k "java -jar server-1.0-SNAPSHOT-jar-with-dependencies.jar n5"

:: Add a small delay to let the server windows initialize before the client.
ECHO Waiting 5 seconds for servers to initialize...
timeout /t 5 /nobreak >nul

ECHO Starting the client CLI...
ECHO.
start "Client CLI" cmd /k "java -jar cli-1.0-SNAPSHOT-jar-with-dependencies.jar"

ECHO.
ECHO All processes have been launched.
ECHO This launcher window can now be closed.
ECHO.

PAUSE