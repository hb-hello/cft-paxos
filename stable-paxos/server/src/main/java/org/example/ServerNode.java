package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ServerNode {

    private static final Logger logger = LogManager.getLogger(ServerNode.class);

    private final int MAJORITY_COUNT = 2;
    private final int OTHER_SERVER_COUNT = 4;
    private final long WAIT_BEFORE_PREPARE_MIN_MILLIS = 200;
    private final long WAIT_BEFORE_PREPARE_MAX_MILLIS = 300;
    private final long LEADER_LIVENESS_TIMEOUT_MILLIS = 500;

    private final String serverId;
    private final ServerState state;
    private final Set<String> otherServerIds;
    private final Timer timerBeforePrepare;
    private final Timer leaderLivenessTimer;
    private final ClientState clientState;
    private final Log log;

    private final CommunicationManager comms;

    public ServerNode(String serverId) {
        this.serverId = serverId;
        this.otherServerIds = Config.getServerIdsExcept(serverId);

//        set up database and log
        this.clientState = new ClientState(serverId);
        this.log = new Log(serverId);

//        set up server state - includes log and ballot
        this.state = new ServerState(serverId);

//        set up timers
        this.timerBeforePrepare = new Timer(getRandom(WAIT_BEFORE_PREPARE_MIN_MILLIS, WAIT_BEFORE_PREPARE_MAX_MILLIS), this::attemptPrepare);
        this.leaderLivenessTimer = new Timer(LEADER_LIVENESS_TIMEOUT_MILLIS, this::transitionToCandidate);

//        set up GRPC communications with other servers and clients
        this.comms = new CommunicationManager(serverId, new MessageService(this));
    }

    public ServerNode(String serverId, boolean active) {
        this(serverId);
        comms.setActive(active);
    }

    private long getRandom(long min, long max) {
        Random random = new Random();
        return random.nextLong(max - min + 1) + min;
    }

    public ServerState getState() {
        return state;
    }

    public String getServerId() {
        return serverId;
    }

    public void setActive(boolean active) {
        comms.setActive(active);
    }

    // triggered whenever backup role's timer expires or when leader id is null (on startup)
    public void transitionToCandidate() {
        logger.info("Transitioning to candidate role");
        state.setRole(Role.CANDIDATE);
        state.setLeaderId(null);
        state.clearPromises();
        attemptPrepare();
    }

    //    triggered when promiseQueue collects a majority quorum of promises
    public void transitionToLeader() {
        logger.info("Transitioning to leader role");
        state.setRole(Role.LEADER);
        state.setLeaderId(serverId);
        timerBeforePrepare.stop();
//        send New View
    }

    //    triggered when a prepare/accept/commit/heartbeat(? -> heartbeat should be treated same as empty accept?) with a higher ballot is received
    public void transitionToBackup(String newLeaderId) {
        logger.info("Transitioning to backup role");
        state.setRole(Role.BACKUP);
        state.setLeaderId(newLeaderId);
        timerBeforePrepare.stop();
        leaderLivenessTimer.start();
    }

    public void attemptPrepare() {
        if (state.isCandidate() && !timerBeforePrepare.isRunning() && comms.isActive()) {
            broadcastPrepare();
        }
    }

    private void broadcastPrepare() {
        try (ExecutorService executor = Executors.newFixedThreadPool(OTHER_SERVER_COUNT)) {
            for (String serverId : otherServerIds) {
                executor.submit(() -> {
                    MessageServiceOuterClass.PromiseMessage promise = comms.sendPrepare(serverId, state.getBallot());
                    handlePromise(promise);
                });
            }
        } catch (Exception e) {
            logger.error("Error when sending prepare messages : {}", e.getMessage());
//            throw new RuntimeException(e);
        }
    }

    private void handlePromise(MessageServiceOuterClass.PromiseMessage promise) {
        if (promise.getBallot().equals(state.getBallot().toProtoBallot())) {
            int promiseCount = state.addPromise(promise);
            if (promiseCount >= MAJORITY_COUNT) {
                transitionToLeader();
            }
        }
    }

    private void handlePrepare(MessageServiceOuterClass.PrepareMessage prepare) {
        timerBeforePrepare.start();
//        check the ballot of prepare against own ballot
//        send promise if needed
    }

    public void addToLog(MessageServiceOuterClass.ClientRequest request) {

//        request would not be in log as we're checking for that before calling addToLog
        log.add(request);

        if (state.isLeader()) {
//            send accept message
        }
    }

    //    to check if the client request is already in the log - don't think we need this function
    public boolean compareTimestampAgainstLog(MessageServiceOuterClass.ClientRequest request) {
        return clientState.getTimestamp(request.getClientId()) < request.getTimestamp();
    }

    public void start() {

//        start listening for requests on a separate thread
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            executor.submit(comms::startListening);

//            wait for server to start
            Thread.sleep(150);

            logger.info("Attempting to start activities for role : {}", state.getRole());

            if (state.getRole() == null) {
                transitionToCandidate();
            }

        } catch (Exception e) {
            logger.error("Error in GRPC server : {}", e.getMessage());
            throw new RuntimeException(e);
        }

    }

    public void shutdown() {
//        shutdown grpc server
//        shutdown all timers
    }

    public static void main(String[] args) {
        Config.initialize();

        if (args.length == 0) {
            logger.error("No server ID provided while initializing server.");
            return;
        }
//        First argument will be server ID
        ServerNode serverNode;
        String serverId = args[0];

        if (args.length == 1) {
            serverNode = new ServerNode(serverId);
        } else {
            serverNode = new ServerNode(serverId, Boolean.parseBoolean(args[1]));
        }
        serverNode.start();
    }
}