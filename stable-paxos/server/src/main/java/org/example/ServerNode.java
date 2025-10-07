package org.example;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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

    // Track pending CLIENT requests (not accept messages)
    private final ConcurrentHashMap<String, PendingRequest> pendingRequests;
    private final AtomicInteger pendingRequestCount;
    private final ConcurrentHashMap<String, MessageServiceOuterClass.PrepareMessage> receivedPrepareMessages;

    private final CommunicationManager comms;

    // Separate ExecutorServices for different concerns
    private final ExecutorService stateExecutor;        // Single-threaded for state transitions
    private final ExecutorService logExecutor;          // Single-threaded for log operations
    private final ExecutorService networkExecutor;      // Multi-threaded for network I/O
    private final ExecutorService messageExecutor;      // Multi-threaded for message processing

    public ServerNode(String serverId) {
        this.serverId = serverId;
        this.otherServerIds = Config.getServerIdsExcept(serverId);

//        set up database and log
        this.clientState = new ClientState(serverId);
        this.log = new Log(serverId);
        this.pendingRequests = new ConcurrentHashMap<>();
        this.pendingRequestCount = new AtomicInteger(0);
        this.receivedPrepareMessages = new ConcurrentHashMap<>();

//        set up server state - includes log and ballot
        this.state = new ServerState(serverId);

//        set up timers
        this.timerBeforePrepare = new Timer(getRandom(WAIT_BEFORE_PREPARE_MIN_MILLIS, WAIT_BEFORE_PREPARE_MAX_MILLIS), this::attemptPrepare);
        this.leaderLivenessTimer = new Timer(LEADER_LIVENESS_TIMEOUT_MILLIS, this::onRequestTimeout);

//        set up GRPC communications with other servers and clients
        this.comms = new CommunicationManager(serverId, new MessageService(this));


        // Initialize separate executors
        // State management: Single-threaded to avoid race conditions on state mutations
        this.stateExecutor = Executors.newSingleThreadExecutor(createNamedThreadFactory("state-manager"));

        // Log management: Single-threaded to maintain sequential consistency of log entries
        this.logExecutor = Executors.newSingleThreadExecutor(createNamedThreadFactory("log-manager"));

        // Network I/O: Fixed thread pool sized for concurrent network operations
        // Size based on: OTHER_SERVER_COUNT * 2 (for send/receive) + buffer
        this.networkExecutor = Executors.newFixedThreadPool(Math.max(10, OTHER_SERVER_COUNT * 2), createNamedThreadFactory("network-io"));

        // Message processing: Cached thread pool that can scale with incoming message load
        this.messageExecutor = Executors.newCachedThreadPool(createNamedThreadFactory("message-processor"));
    }

    /**
     * Creates a ThreadFactory that produces named threads for better debugging
     * and monitoring. Named threads help identify thread pool types in thread dumps.
     */
    private ThreadFactory createNamedThreadFactory(String poolName) {
        return new ThreadFactory() {
            private int counter = 0;

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(serverId + "-" + poolName + "-" + counter++);
                thread.setDaemon(false); // Ensure proper shutdown control
                return thread;
            }
        };
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

    public boolean isActive() {
        return comms.isActive();
    }

    public String getServerId() {
        return serverId;
    }

    public void setActive(boolean active) {
        comms.setActive(active);
    }

    //    Called when request timer expires - node has been waiting too long to execute a request
    private void onRequestTimeout() {
        logger.warn("Request timeout! {} pending CLIENT requests not executed", pendingRequestCount.get());

        if (pendingRequestCount.get() > 0 && !state.isLeader()) {
            logger.info("Attempting to become leader to process pending client requests");
            // Submit to state executor for thread-safe state transition
            stateExecutor.submit(() -> {
                // process logged prepares

                // If role is backup and leader seems dead, consider becoming candidate
                if (state.isBackup() || state.getLeaderId() != null) {
                    logger.info("Leader suspected dead, transitioning to candidate");
                    transitionToCandidate();
                }
            });
        }
    }

    // triggered whenever backup role's timer expires or when leader id is null (on startup)
    public void transitionToCandidate() {
        logger.info("Transitioning to candidate role");

        state.setRole(Role.CANDIDATE);
        state.setLeaderId(null);
        state.clearPromises();

        if (leaderLivenessTimer.isRunning()) leaderLivenessTimer.stop();

        logger.info("Role: CANDIDATE, Ballot: {}, LeaderId: null", state.getBallot());

        attemptPrepare();
    }

    //    triggered when promiseQueue collects a majority quorum of promises
    public void transitionToLeader() {
        logger.info("Transitioning to leader role");

        state.setRole(Role.LEADER);
        state.setLeaderId(serverId);
        timerBeforePrepare.stop();
        leaderLivenessTimer.stop();

        logger.info("Role: LEADER, Ballot: {}, LeaderId: {}", state.getBallot(), serverId);

//        Process accept log (uses logExecutor internally) and send New View
    }

    //    triggered when a prepare/accept/commit/heartbeat(? -> heartbeat should be treated same as empty accept?) with a higher ballot is received
    public void transitionToBackup(String newLeaderId, Ballot newBallot) {
        logger.info("Transitioning to backup role");

        state.setRole(Role.BACKUP);
        state.setLeaderId(newLeaderId);
        state.setBallot(newBallot);
        state.clearPromises();
        timerBeforePrepare.stop();
//        Start timer to monitor leader
        leaderLivenessTimer.startOrRefresh();

        logger.info("Role: BACKUP, Ballot: {}, LeaderId: {}", newBallot, newLeaderId);

    }

    public void attemptPrepare() {
        logger.info("Attempting to send prepare ({} & {} & {})", state.isCandidate() || state.getLeaderId() == null, !timerBeforePrepare.isRunning(), comms.isActive());

        if (!comms.isActive()) {
            logger.debug("Server inactive, skipping prepare attempt");
            return;
        }

        // Submit to state executor for thread-safe state check
        stateExecutor.submit(() -> {
            // First, check if there are any logged prepare messages to process
            if (!receivedPrepareMessages.isEmpty()) {
                processLoggedPrepareMessages();
                return; // Don't start our own prepare
            }

            // No logged prepares - attempt our own prepare if conditions are right
            if ((state.isCandidate() || state.getLeaderId() == null) && !timerBeforePrepare.isRunning()) {
                logger.info("Starting prepare phase");
                state.incrementBallot();
                broadcastPrepare();
            }
        });
    }

    private void broadcastPrepare() {

        Ballot currentBallot = state.getBallot();

        for (String serverId : otherServerIds) {
            networkExecutor.submit(() -> {
                try {
                    MessageServiceOuterClass.PromiseMessage promise = comms.sendPrepare(serverId, currentBallot);

                    if (promise != null) {
                        logger.info("MESSAGE: <PROMISE, <{}, {}>, acceptLog ({} items)> received from server {}", promise.getSenderId(), promise.getBallot().getInstance(), promise.getBallot().getSenderId(), promise.getAcceptLogList().size());

                        // Process promise in message executor
                        messageExecutor.submit(() -> handlePromise(promise));
                    }
                } catch (Exception e) {
                    logger.error("Error sending prepare to {}: {}", serverId, e.getMessage());
                }
            });
        }
    }

    private void handlePromise(MessageServiceOuterClass.PromiseMessage promise) {
        Ballot incomingBallot = Ballot.fromProtoBallot(promise.getBallot());

        // Submit to state executor for thread-safe state access
        stateExecutor.submit(() -> {
//            Ignore promises with different ballots
            if (!incomingBallot.equals(state.getBallot()) || !state.isCandidate()) {
                return;
            }

//            calculate quorum of promises
            int promiseCount = state.addPromise(promise);
            logger.info("Promise count: {}/{} (majority: {})", promiseCount, OTHER_SERVER_COUNT + 1, MAJORITY_COUNT);

            if (promiseCount >= MAJORITY_COUNT && state.isCandidate()) {
                logger.info("Majority quorum achieved ({}/{})", promiseCount, MAJORITY_COUNT);
                transitionToLeader(); // safe as it is within stateExecutor
            }
        });
    }

    public MessageServiceOuterClass.PromiseMessage handlePrepare(MessageServiceOuterClass.PrepareMessage prepare) {

        Ballot incomingBallot = Ballot.fromProtoBallot(prepare.getBallot());

        // Submit to message executor first, then coordinate with state executor and eventually use logExecutor to access logs and create the promise
        CompletableFuture<MessageServiceOuterClass.PromiseMessage> responseFuture = CompletableFuture.supplyAsync(() -> {
            return logPrepareIfLeaderIsAlive(incomingBallot, prepare);
        }, messageExecutor).thenComposeAsync(ballot -> {
            if (ballot == null) {
                return CompletableFuture.completedFuture(null);
            }
            // Leader suspected dead, proceed with comparing ballot
            return CompletableFuture.supplyAsync(() -> {
                return checkBallotIfCandidate(ballot);
            }, stateExecutor).thenCombineAsync(CompletableFuture.supplyAsync(log::getAcceptLog, logExecutor), this::createPromise);
        }, stateExecutor);

        try {
            return responseFuture.get(500, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Error processing prepare: {}", e.getMessage());
            return null;
        }


    }

    private Ballot logPrepareIfLeaderIsAlive(Ballot incomingBallot, MessageServiceOuterClass.PrepareMessage prepare) {
        boolean leaderIsAlive = leaderLivenessTimer.isRunning();

        if (leaderIsAlive) {
            // Leader is still considered alive - log this prepare message but don't respond
            logger.info("Leader still considered alive - logging prepare from {}", incomingBallot.getServerId());
            receivedPrepareMessages.put(incomingBallot.getServerId(), prepare);
            return null;
        }

        logger.info("Leader suspected dead - processing prepare immediately");
        return incomingBallot;
    }

    private Ballot checkBallotIfCandidate(Ballot incomingBallot) {

        if (state.isCandidate() || state.getLeaderId() == null) {
            timerBeforePrepare.startOrRefresh();
        }

        Ballot currentBallot = state.getBallot();

        if (incomingBallot.isGreaterThan(currentBallot)) {
            state.setBallot(incomingBallot);
            return incomingBallot;
        } else return null;

    }

    //    Process all logged prepare messages and choose the highest ballot to send a promise
    private void processLoggedPrepareMessages() {
        if (receivedPrepareMessages.isEmpty()) {
            logger.debug("No logged prepare messages to process");
            return;
        }

        // Find the prepare message with the highest ballot
        MessageServiceOuterClass.PrepareMessage highestPrepare = null;
        Ballot highestBallot = state.getBallot();

        for (MessageServiceOuterClass.PrepareMessage prepare : receivedPrepareMessages.values()) {
            Ballot ballot = Ballot.fromProtoBallot(prepare.getBallot());
            if (ballot.isGreaterThan(highestBallot)) {
                highestBallot = ballot;
                highestPrepare = prepare;
            }
        }

        // Accept the highest ballot prepare
        if (highestPrepare != null) {
            Ballot acceptedBallot = Ballot.fromProtoBallot(highestPrepare.getBallot());

            state.setBallot(acceptedBallot);
        }

        // Clear logged prepares
        receivedPrepareMessages.clear();

    }

    private MessageServiceOuterClass.PromiseMessage createPromise(Ballot ballot, List<MessageServiceOuterClass.AcceptMessage> acceptLog) {
        if (ballot == null) {
            return null;
        }
        logger.info("MESSAGE: <PROMISE, <{}, {}>, acceptLog ({} items)> sent to server {}", ballot.getTerm(), ballot.getServerId(), acceptLog.size(), ballot.getServerId());
        return MessageServiceOuterClass.PromiseMessage.newBuilder().addAllAcceptLog(acceptLog).setBallot(ballot.toProtoBallot()).setSenderId(serverId).build();
    }

    private String getRequestKey(MessageServiceOuterClass.ClientRequest request) {
        return request.getClientId() + "-" + request.getTimestamp();
    }

    //    Handle incoming client request - add to pendingRequests and manage the timer
    public void handleClientRequest(MessageServiceOuterClass.ClientRequest request, StreamObserver<MessageServiceOuterClass.ClientReply> responseObserver) {
        String requestKey = getRequestKey(request);

        // Submit to message executor for processing
        messageExecutor.submit(() -> {
            // Check if request is already executed (reads from clientState)
            if (!compareTimestampAgainstLog(request)) {
                logger.info("Request already executed, ignoring");
                return;
            }

            // Check current state and route accordingly
            stateExecutor.submit(() -> {
                if (state.isLeader()) {
                    logger.info("Processing request immediately");
//                    logExecutor.submit(() -> processClientRequest(request));
                    return;
                }

                String currentLeader = state.getLeaderId();
                if (currentLeader != null) {
                    logger.info("Forwarding request to leader: {}", currentLeader);
//                    forwardRequestToLeader(request, currentLeader);
                    return;
                }

                // No leader - add to pending and start timer
                pendingRequests.put(requestKey, new PendingRequest(request, responseObserver));
                int pendingCount = pendingRequestCount.incrementAndGet();

                logger.info("No leader - added to pending requests. Total pending: {}", pendingCount);

                if (state.getRole() == null) {
                    transitionToCandidate();
                }

                if (!leaderLivenessTimer.isRunning() && state.isBackup()) {
                    logger.info("Starting leader liveness timer ({}ms)", LEADER_LIVENESS_TIMEOUT_MILLIS);
                    leaderLivenessTimer.startOrRefresh();
                }
            });

        });
    }

    public void addToLog(MessageServiceOuterClass.ClientRequest request) {

//        request would not be in log as we're checking for that before calling addToLog
        log.add(request, state.getBallot());

        if (state.isLeader()) {
//            send accept message
        }
    }

    //    to check if the client request is already in the log - don't think we need this function
    public boolean compareTimestampAgainstLog(MessageServiceOuterClass.ClientRequest request) {
        return clientState.getTimestamp(request.getClientId()) < request.getTimestamp();
    }

    //    apply the accept messages from accept log received in promises, to self log
    private void processOthersAcceptMessage(MessageServiceOuterClass.AcceptMessage acceptMessage) {
        long seqNum = acceptMessage.getSequenceNumber();

    }

    public void start() {

//        start listening for requests on a separate thread
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            executor.submit(comms::startListening);

//            wait for server to start
            Thread.sleep(100);

            logger.info("Attempting to start activities for role : {}", state.getRole());

//            if (state.getRole() == null) {
//                transitionToCandidate();
//            }

            // Block main thread to keep app running
            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                logger.error("Main thread interrupted: {}", e.getMessage());
                Thread.currentThread().interrupt();
            }

        } catch (Exception e) {
            logger.error("Error in GRPC server : {}", e.getMessage());
            throw new RuntimeException(e);
        }

    }

    private void shutdownExecutor(ExecutorService executor, String name) {
        logger.info("Shutting down {} executor", name);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("{} executor did not terminate in time, forcing shutdown", name);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.error("{} executor shutdown interrupted", name);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public void shutdown() {
        logger.info("Shutting down ServerNode: {}", serverId);

        timerBeforePrepare.shutdown();
        leaderLivenessTimer.shutdown();  // Add this

        shutdownExecutor(networkExecutor, "Network");
        shutdownExecutor(messageExecutor, "Message");
        shutdownExecutor(logExecutor, "Log");
        shutdownExecutor(stateExecutor, "State");

        comms.shutdown();

        logger.info("ServerNode shutdown complete");
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

        Runtime.getRuntime().addShutdownHook(new Thread(serverNode::shutdown));

        serverNode.start();
    }
}