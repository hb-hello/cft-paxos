package org.example;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;

import static org.example.Log.getRequestKey;

public class ServerNode {

    private static final Logger logger = LogManager.getLogger(ServerNode.class);

    private final int MAJORITY_COUNT = 2;
    private final int OTHER_SERVER_COUNT = 4;
    private final long WAIT_BEFORE_PREPARE_MIN_MILLIS = 500;
    private final long WAIT_BEFORE_PREPARE_MAX_MILLIS = 700;
    private final long REQUEST_TIMEOUT_MILLIS = 1000;

    private final String serverId;
    private final ServerState state;
    private final Set<String> otherServerIds;
    private final Timer timerBeforePrepare;
    private final Timer requestTimer;
    private final ClientState clientState;
    private final Log log;

    // Track pending client requests
    private final ConcurrentHashMap<String, PendingRequest> pendingRequests;
    private final Set<String> requestsIncludedInNewView;
    private final ConcurrentHashMap<String, Boolean> requestsCommitted;
    private final ConcurrentHashMap<String, Long> requestsAccepted;


    //    Track received prepare messages
    private final ConcurrentHashMap<String, LoggedPrepareMessage> receivedPrepareMessages;

    // Track client requests
    private final ConcurrentHashMap<String, MessageServiceOuterClass.ClientReply> replyCache;

    //    Handle GRPC communications
    private final CommunicationManager comms;

    // Separate ExecutorServices for different concerns
    private final ExecutorService stateExecutor;        // Single-threaded for state transitions
    private final ExecutorService logExecutor;          // Single-threaded for log operations
    private final ExecutorService networkExecutor;      // Multi-threaded for network I/O
    private final ExecutorService streamingExecutor;    // Dedicated for streaming operations (NewView)
    private final ExecutorService messageExecutor;      // Multi-threaded for message processing

    public ServerNode(String serverId) {
        this.serverId = serverId;
        this.otherServerIds = Config.getServerIdsExcept(serverId);

//        set up database and log
        this.clientState = new ClientState(serverId);
        this.log = new Log(serverId);
        this.pendingRequests = new ConcurrentHashMap<>();
        requestsIncludedInNewView = ConcurrentHashMap.newKeySet();
        this.receivedPrepareMessages = new ConcurrentHashMap<>();
        this.replyCache = new ConcurrentHashMap<>();
        this.requestsCommitted = new ConcurrentHashMap<>();
        this.requestsAccepted = new ConcurrentHashMap<>();


//        set up server state - includes log and ballot
        this.state = new ServerState(serverId);

//        set up timers
        this.timerBeforePrepare = new Timer(getRandom(WAIT_BEFORE_PREPARE_MIN_MILLIS, WAIT_BEFORE_PREPARE_MAX_MILLIS), this::transitionToCandidate);
        this.requestTimer = new Timer(REQUEST_TIMEOUT_MILLIS, this::onRequestTimeout);

//        set up GRPC communications with other servers and clients
        this.comms = new CommunicationManager(serverId, new MessageService(this));


        // Initialize separate executors
        // State management: Single-threaded to avoid race conditions on state mutations
        this.stateExecutor = Executors.newSingleThreadExecutor(createNamedThreadFactory("state-manager"));

        // Log management: Single-threaded to maintain sequential consistency of log entries
        this.logExecutor = Executors.newSingleThreadExecutor(createNamedThreadFactory("log-manager"));

        // Network I/O: Fixed thread pool sized for concurrent network operations
        // Size based on: OTHER_SERVER_COUNT * 2 (for send/receive) + buffer
//        this.networkExecutor = createMonitoredNetworkExecutor();
        this.networkExecutor = Executors.newFixedThreadPool(
                Math.max(50, OTHER_SERVER_COUNT * 2),
                createNamedThreadFactory("network-io")
        );

        // Streaming operations: Fixed thread pool for long-running streaming RPCs
        // Sized for concurrent NewView operations to all servers
        // These operations hold connections open and process streaming responses
        this.streamingExecutor = Executors.newFixedThreadPool(
                Math.max(5, OTHER_SERVER_COUNT),
                createNamedThreadFactory("streaming-io")
        );

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

    public List<String> getCommunicationLog() {
        return comms.getCommunicationLog();
    }

    public void logCommunication(String message) {
        comms.logCommunication(message);
    }

    public Log getLog() {
        return log;
    }

    private long getRandom(long min, long max) {
        Random random = new Random();
        return random.nextLong(max - min + 1) + min;
    }

    public ServerState getState() {
        return state;
    }

    public List<MessageServiceOuterClass.NewViewMessage> getNewViews() {
        return comms.getNewViews();
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
        logger.warn("Request timeout! {} pending CLIENT requests not executed", pendingRequests.size());

        if (!pendingRequests.isEmpty()) {
            if (!state.isLeader()) {
                logger.info("Attempting to become leader to process pending client requests");
                // Submit to state executor for thread-safe state transition
                stateExecutor.submit(() -> {
                    // If role is backup and leader seems dead, consider becoming candidate
                    logger.info("Current role: {}, LeaderId: {}", state.getRole(), state.getLeaderId());
                    if (state.isBackup() || state.getLeaderId() == null) {
                        logger.info("Leader suspected dead, transitioning to candidate");
                        transitionToCandidate();
                    }
                });
            } else {
                executeAllCommittedInOrder();
            }
        }
    }

    // Role changes------------------------------------------------------------------------------------------------------------

    // triggered whenever backup role's timer expires or when leader id is null (on startup)
    public void transitionToCandidate() {
        logger.info("Transitioning to candidate role");

        state.setRole(Role.CANDIDATE);
        state.setLeaderId(null);

//        This will keep trying to start leader elections to execute all transactions it has received
        stopOrRefreshRequestTimer();
//        requestTimer.stop();

        logger.info("Role: CANDIDATE, Ballot: {}, LeaderId: null", state.getBallot());

        if (!timerBeforePrepare.isRunning()) attemptPrepare();
    }

    private void stopOrRefreshRequestTimer() {
        if (pendingRequests.isEmpty() || !comms.isActive()) {
            requestTimer.stop();
            logger.info("Leader liveness timer stopped as there are no pending requests or server is inactive");
        } else {
            requestTimer.startOrRefresh();
            logger.info("Leader liveness timer refreshed as there are {} pending requests", pendingRequests.size());
        }
    }

    //    triggered when promiseQueue collects a majority quorum of promises
    public void transitionToLeader() {
        logger.info("Transitioning to leader role");

        state.setRole(Role.LEADER);
        state.setLeaderId(serverId);
        timerBeforePrepare.stop();

//        if (!pendingRequests.isEmpty()) requestTimer.startOrRefresh();

        logger.info("Role: LEADER, Ballot: {}, LeaderId: {}", state.getBallot(), serverId);

        processAcceptLogFromPromises();

        Ballot ballot = state.getBallot();
        logExecutor.submit(() -> {
            logger.info("Attempting to Broadcasting NEW VIEW for ballot {}", ballot);
            broadcastNewView(ballot);
            processPendingRequests();
        });
    }

    //    triggered when a prepare/accept/commit/heartbeat(? -> heartbeat should be treated same as empty accept?) with a higher ballot is received
    public void transitionToBackup(String newLeaderId, Ballot newBallot) {
        logger.info("Transitioning to backup role");

        state.setRole(Role.BACKUP);
        state.setLeaderId(newLeaderId);
        state.setBallot(newBallot);
        timerBeforePrepare.stop();
//        Start timer to monitor leader
        requestTimer.startOrRefresh();

        logger.info("Role: BACKUP, Ballot: {}, LeaderId: {}", newBallot, newLeaderId);

    }

    // LEADER ELECTION phase - prepare and promise handling--------------------------------------------------------------------

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
        state.clearPromises();

        for (String serverId : otherServerIds) {
            networkExecutor.submit(() -> {
                try {
                    MessageServiceOuterClass.PromiseMessage promise = comms.sendPrepare(serverId, currentBallot);

                    if (promise != null) {
                        logCommunication(
                                String.format("MESSAGE: <PROMISE, <%d, %s>, acceptLog (%d items)> received from server %s", promise.getBallot().getInstance(), promise.getBallot().getSenderId(), promise.getAcceptLogList().size(), promise.getSenderId())
                        );
                        logger.info("MESSAGE: <PROMISE, <{}, {}>, acceptLog ({} items)> received from server {}", promise.getBallot().getInstance(), promise.getBallot().getSenderId(), promise.getAcceptLogList().size(), promise.getSenderId());

                        // Process promise in message executor
                        messageExecutor.submit(() -> handlePromise(promise));
                    }
                } catch (StatusRuntimeException e) {
                    if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED)
                        logger.error("Prepare request timed out for server {} - it might be inactive",
                                serverId);
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
            logger.info("Should ignore promise when ballot is {}? - {} or {}", state.getBallot().toString(), !incomingBallot.equals(state.getBallot()), !state.isCandidate());
            if (!incomingBallot.equals(state.getBallot()) || !state.isCandidate()) {
                logger.info("Ignoring promise : {} | {}, role is {}", !incomingBallot.equals(state.getBallot()), !state.isCandidate(), state.getRole());
                return;
            }

//            Calculate quorum of promises
            int promiseCount = state.addPromise(promise);
            logger.info("Promise count: {}/{} (majority: {})", promiseCount, OTHER_SERVER_COUNT, MAJORITY_COUNT);

            if (promiseCount >= MAJORITY_COUNT && state.isCandidate()) {
                logger.info("Majority quorum achieved ({}/{})", promiseCount, MAJORITY_COUNT);
                transitionToLeader(); // safe as it is within stateExecutor
            }
        });
    }

    private void processAcceptLogFromPromises() {
        List<MessageServiceOuterClass.PromiseMessage> promises = state.getPromises();

        logger.info("Processing accept logs from {} promises", promises.size());

        Ballot currentBallot = state.getBallot();

        for (MessageServiceOuterClass.PromiseMessage promise : promises) {
            for (MessageServiceOuterClass.AcceptMessage acceptMessage : promise.getAcceptLogList()) {
                processOthersAcceptMessage(acceptMessage, currentBallot);
            }
        }
    }

    //    Process all pending client requests that were collected before becoming leader
    private void processPendingRequests() {
        if (pendingRequests.isEmpty()) {
            logger.info("No pending client requests to process");
            return;
        }

        logger.info("Processing {} pending client requests as new leader", pendingRequests.size());

        for (PendingRequest pendingRequest : pendingRequests.values()) {
            String key = Log.getRequestKey(pendingRequest.request());
            if (requestsIncludedInNewView.contains(key)) {
                // Already in NewView: skip this one
                continue;
            }
            logger.info("Processing pending request -> {}", getRequestKey(pendingRequest.request()));
            logExecutor.submit(() -> processClientRequest(pendingRequest.request(), true));
        }

        // Clear all pending requests
//        pendingRequests.clear();
//        requestTimer.stop();

        logger.info("Finished processing pending client requests");
    }

    //    apply the accept messages from accept log received in promises, to self log
    private void processOthersAcceptMessage(MessageServiceOuterClass.AcceptMessage acceptMessage, Ballot currentBallot) {
        long seqNum = acceptMessage.getSequenceNumber();
        Ballot acceptBallot = Ballot.fromProtoBallot(acceptMessage.getBallot());
        LogEntry existingEntry = log.getLogEntry(seqNum);

        if (existingEntry == null) {
            log.setLogEntry(acceptMessage.getSequenceNumber(), currentBallot, acceptMessage.getRequest());
            clientState.setTimestamp(acceptMessage.getRequest().getClientId(), acceptMessage.getRequest().getTimestamp());
            logger.info("Applied accept from promise to log at seq={}", seqNum);
        } else if (acceptBallot.isGreaterThan(existingEntry.getBallot())) {
            log.setLogEntry(seqNum, currentBallot, existingEntry.getRequest());
            logger.info("Updated ballot from accept in promise in log at seq={}", seqNum);
        } else {
            logger.debug("Accept from promise already in log at seq={}", seqNum);
        }

    }

    public MessageServiceOuterClass.PromiseMessage handlePrepare(MessageServiceOuterClass.PrepareMessage prepare, StreamObserver<MessageServiceOuterClass.PromiseMessage> responseObserver) {

        Ballot incomingBallot = Ballot.fromProtoBallot(prepare.getBallot());

        CompletableFuture<MessageServiceOuterClass.PromiseMessage> promiseFuture = CompletableFuture.supplyAsync(() -> {
            timerBeforePrepare.startOrRefresh();

            // 1. Check leader liveness (now safely on the stateExecutor)
            boolean leaderIsAlive = requestTimer.isRunning();
            if (leaderIsAlive && state.isBackup()) {
                logger.info("Leader still considered alive - logging prepare from {}", incomingBallot.getServerId());
                receivedPrepareMessages.put(incomingBallot.getServerId(), new LoggedPrepareMessage(prepare, responseObserver));
                return null; // Don't respond yet
            }

            // 2. Leader suspected dead, proceed with ballot check
            logger.info("Leader suspected dead - processing prepare immediately");
            Ballot currentBallot = state.getBallot();
            if (incomingBallot.isGreaterThan(currentBallot)) {

                if (state.isLeader()) {
                    transitionToCandidate();
                }

                state.setBallot(incomingBallot);

                // 3. Get the accept log and create the promise
                // Since this is a blocking call inside a CompletableFuture, it's okay.
                List<MessageServiceOuterClass.AcceptMessage> acceptLog = log.getAcceptLog();
                return createPromise(incomingBallot, acceptLog);
            }

            return null; // Ballot not high enough
        }, stateExecutor);

        try {
            return promiseFuture.get(500, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Error processing prepare: {}", e.getMessage());
            return null;
        }
    }

    //    Process all logged prepare messages and choose the highest ballot to send a promise
    private void processLoggedPrepareMessages() {
        if (receivedPrepareMessages.isEmpty()) {
            logger.debug("No logged prepare messages to process");
            return;
        }

        // Find the prepare message with the highest ballot
        LoggedPrepareMessage highestLoggedPrepare = null;
        Ballot highestBallot = state.getBallot();

        for (LoggedPrepareMessage loggedPrepareMessage : receivedPrepareMessages.values()) {
            Ballot ballot = Ballot.fromProtoBallot(loggedPrepareMessage.prepareMessage().getBallot());
            if (ballot.isGreaterThan(highestBallot)) {
                highestBallot = ballot;
                highestLoggedPrepare = loggedPrepareMessage;
            }
        }

        // Accept the highest ballot prepare
        if (highestLoggedPrepare != null) {
            Ballot acceptedBallot = Ballot.fromProtoBallot(highestLoggedPrepare.prepareMessage().getBallot());
            state.setBallot(acceptedBallot);

            List<MessageServiceOuterClass.AcceptMessage> acceptLog = log.getAcceptLog();
            MessageServiceOuterClass.PromiseMessage promise = createPromise(acceptedBallot, acceptLog);

            highestLoggedPrepare.responseObserver().onNext(promise);
            highestLoggedPrepare.responseObserver().onCompleted();
        }

//        #TODO: access the streamobserver for highest prepare and send promise

        // Clear logged prepares
        receivedPrepareMessages.clear();

    }

    private MessageServiceOuterClass.PromiseMessage createPromise(Ballot ballot, List<MessageServiceOuterClass.AcceptMessage> acceptLog) {
        if (ballot == null) {
            return null;
        }

        Ballot currentBallot = state.getBallot();

//        Check again that ballot is still the highest
        if (currentBallot.isGreaterThan(ballot)) {
            logger.info("Current ballot is now higher than incoming ballot, not sending promise");
            return null;
        }

        logCommunication(
                String.format("MESSAGE: <PROMISE, <%d, %s>, acceptLog (%d items)> sent to server %s", ballot.getTerm(), ballot.getServerId(), acceptLog.size(), ballot.getServerId())
        );
        logger.info("MESSAGE: <PROMISE, <{}, {}>, acceptLog ({} items)> sent to server {}", ballot.getTerm(), ballot.getServerId(), acceptLog.size(), ballot.getServerId());
        return MessageServiceOuterClass.PromiseMessage.newBuilder().addAllAcceptLog(acceptLog).setBallot(ballot.toProtoBallot()).setSenderId(serverId).build();
    }

    // NORMAL OPERATIONS phase - prepare and promise handling------------------------------------------------------------------

    //    Handle incoming client request - add to pendingRequests and refresh the timer
    public void handleClientRequest(MessageServiceOuterClass.ClientRequest request, StreamObserver<MessageServiceOuterClass.ClientReply> responseObserver) {
        String requestKey = getRequestKey(request);

        // Submit to message executor for processing
        messageExecutor.submit(() -> {
            // Check if request is already executed (reads from clientState)
            logger.info("Is timestamp {} less than or equal to log timestamp {}? {}", request.getTimestamp(), clientState.getTimestamp(request.getClientId()), isTimestampLessThanOrEqualToLog(request));
            if (isTimestampLessThanOrEqualToLog(request)) {
                logger.info("Request already executed, ignoring");
//                check in replyCache
                if (replyCache.containsKey(requestKey) && responseObserver != null) {
                    logger.info("Request {} found in reply cache, replying to client.", requestKey);
                    MessageServiceOuterClass.ClientReply reply = replyCache.get(requestKey);
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                }
                return;
            }

            // Check current state and route accordingly
            stateExecutor.submit(() -> {
                boolean isLeader = state.isLeader();
                boolean hasLeader = state.getLeaderId() != null;
                Role role = state.getRole();

//                kick-off leader election phase if we're in the system init phase meaning role will be null
                if (role == null) {
                    transitionToCandidate();
                }

                if (!addToPendingRequests(request, responseObserver, requestKey)) return;

//                Every request added to pending requests is added to log if role is leader or candidate
                if (isLeader) {
                    logger.info("Processing request immediately as {} and sending to processClientRequest for log updates", isLeader ? "leader" : "candidate");
                    logExecutor.submit(() -> processClientRequest(request, isLeader));
                    return;
                }

                if (hasLeader && state.isBackup()) {
                    // has to be backup
                    logger.info("Forwarding request to leader");
                    forwardRequestToLeader(request, state.getLeaderId());
                }

            });

        });
    }

    private boolean addToPendingRequests(MessageServiceOuterClass.ClientRequest request, StreamObserver<MessageServiceOuterClass.ClientReply> responseObserver, String requestKey) {
        // Add new client requests to pending requests to keep track of the response observer to later reply to the client
        if (!pendingRequests.containsKey(requestKey)) {
            // Refresh timer when seeing a client request for the first time
            logger.info("Starting / Refreshing leader liveness timer ({}ms)", REQUEST_TIMEOUT_MILLIS);
            requestTimer.startOrRefresh();
            pendingRequests.put(requestKey, new PendingRequest(request, responseObserver));

            logger.info("Request {} added to pending requests. Total pending: {}", requestKey, pendingRequests.size());
        } else {
            logger.info("Request {} found in pending requests already, ignoring", requestKey);
            return false;
        }
        return true;
    }

    private void removeFromPendingRequests(String requestKey) {
        // Remove from pendingRequests
        pendingRequests.remove(requestKey);
        logger.info("Request {} removed from pending requests. Total pending: {}", requestKey, pendingRequests.size());
        if (pendingRequests.isEmpty()) {
            requestTimer.stop();
        } else requestTimer.startOrRefresh();
    }

    //    Only adds to log if role is candidate; adds to log and broadcasts accept requests if role is leader
    public void processClientRequest(MessageServiceOuterClass.ClientRequest request, boolean sendAccept) {
//        Add request to log
        logger.info("Processing client request {} as {} and sending accept? {}", getRequestKey(request), sendAccept ? "leader" : "candidate", sendAccept);

        if (requestsAccepted.containsKey(getRequestKey(request))) {
            logger.info("Request {} already accepted at seq={}, ignoring", getRequestKey(request), requestsAccepted.get(getRequestKey(request)));
            return;
        }

        long seqNum = log.add(request, state.getBallot()); // safe as this is called within a stateExecutor
        clientState.setTimestamp(request.getClientId(), request.getTimestamp());
        logger.info("Added client request {} to log at sequence: {}", getRequestKey(request), seqNum);

        LogEntry entry = log.getLogEntry(seqNum);
        if (entry == null) {
            logger.error("Failed to retrieve entry at seq={} after adding!", seqNum);
            return;
        }

        requestsAccepted.put(getRequestKey(request), seqNum);

        logger.info("Entry at seq={}: acceptedVotes={}, sendAccept={}",
                seqNum, entry.getAcceptedVotes(), sendAccept);

//        if (log.getLogEntry(seqNum).getAcceptedVotes() == 0 && sendAccept) broadcastAccept(request, seqNum);

        if (entry.getAcceptedVotes() == 0 && sendAccept) {
            logger.info("Broadcasting ACCEPT for seq={}", seqNum);
            broadcastAccept(request, seqNum);
        } else {
            logger.warn("NOT broadcasting ACCEPT: acceptedVotes={}, sendAccept={}",
                    entry.getAcceptedVotes(), sendAccept);
        }

//        CompletableFuture.supplyAsync(state::isLeader, stateExecutor).thenAccept((isLeader) -> {
//            if (isLeader) {
//                broadcastAccept(request, seqNum);
//            }
//        });
    }

    public void broadcastNewView(Ballot ballot) {

        Set<String> keysInNewView = ConcurrentHashMap.newKeySet();

        // Collect all accepted log entries and their keys
        List<MessageServiceOuterClass.AcceptMessage> acceptLog = new ArrayList<>();
        for (MessageServiceOuterClass.AcceptMessage logEntry : log.getAcceptLog()) {
            String key = Log.getRequestKey(logEntry.getRequest());
            acceptLog.add(logEntry);
            keysInNewView.add(key);
        }

        // Take a snapshot of pending requests
        List<PendingRequest> snapshotPendingRequests = new ArrayList<>(pendingRequests.values());

//        Add only new (non-duplicate) snapshot requests, and record their keys for later dedup
//        for (PendingRequest pending : snapshotPendingRequests) {
//            String key = Log.getRequestKey(pending.request());
//            if (keysInNewView.add(key)) { // add returns false if already present
//                long seqNum = log.add(pending.request(), ballot);
//                MessageServiceOuterClass.AcceptMessage acceptMsg =
//                        MessageServiceOuterClass.AcceptMessage.newBuilder()
//                                .setBallot(ballot.toProtoBallot())
//                                .setSequenceNumber(seqNum)
//                                .setRequest(pending.request())
//                                .build();
//                clientState.setTimestamp(pending.request().getClientId(), pending.request().getTimestamp());
//                acceptLog.add(acceptMsg);
//            }
//        }

        // Clear and update the set of included keys
        requestsIncludedInNewView.clear();
        requestsIncludedInNewView.addAll(keysInNewView);

//        Build NewView message
        MessageServiceOuterClass.NewViewMessage newViewMessage =
                MessageServiceOuterClass.NewViewMessage.newBuilder()
                        .setBallot(ballot.toProtoBallot())
                        .addAllAcceptLog(acceptLog)
                        .build();

        logger.info("Broadcasting NewView with {} accept log entries to {} servers",
                acceptLog.size(), otherServerIds.size());

        comms.addNewView(newViewMessage);

        // Send NewView using streamingExecutor (NOT networkExecutor)
        // Each streaming RPC needs its own thread to handle the stream of responses
        for (String serverId : otherServerIds) {
            streamingExecutor.submit(() -> {
                try {
                    logger.info("Starting NewView stream to server {}", serverId);
                    comms.sendNewView(serverId, newViewMessage, this::handleAccepted);
                    logger.info("Completed NewView stream to server {}", serverId);
                } catch (Exception e) {
                    logger.error("Error in NewView stream to server {}: {}", serverId, e.getMessage(), e);
                }
            });
        }

        logger.info("Submitted all NewView streams to streaming executor");

//        CompletableFuture<MessageServiceOuterClass.NewViewMessage> newViewBuilderFuture = CompletableFuture.supplyAsync(() -> MessageServiceOuterClass.NewViewMessage.newBuilder().addAllAcceptLog(log.getAcceptLog()).setBallot(ballot.toProtoBallot()).build(), logExecutor);
//        newViewBuilderFuture.thenAccept(newViewMessage -> {
//            for (String targetServerId : otherServerIds) {
//                comms.sendNewView(targetServerId, newViewMessage, this::handleAccepted);
//            }
//        });

    }

    public void handleNewView(MessageServiceOuterClass.NewViewMessage newViewMessage, StreamObserver<MessageServiceOuterClass.AcceptedMessage> responseObserver) {

        Ballot currentBallot = state.getBallot();
        Ballot newViewBallot = Ballot.fromProtoBallot(newViewMessage.getBallot());

        if (!newViewBallot.isGreaterThanOrEqual(currentBallot) || (state.isLeader() && requestTimer.isRunning())) {
            logger.info("Ignoring NewView with lower ballot: current={}, newView={}", currentBallot, newViewBallot);
            return;
        }

        streamingExecutor.submit(() -> {
            try {
                for (int i = 0; i < newViewMessage.getAcceptLogCount(); i++) {
                    MessageServiceOuterClass.AcceptMessage acceptMessage = newViewMessage.getAcceptLog(i);
                    logger.info("Processing accept message {}/{} from NewView", i + 1, newViewMessage.getAcceptLogCount());

                    MessageServiceOuterClass.AcceptedMessage accepted = handleAccept(acceptMessage);
                    if (accepted != null) {
                        responseObserver.onNext(accepted);
                    }
                }
                responseObserver.onCompleted();
                logger.info("Completed sending all {} accepted messages for NewView", newViewMessage.getAcceptLogCount());
            } catch (Exception e) {
                logger.error("Error handling NewView: {}", e.getMessage(), e);
                responseObserver.onError(e);
            }
        });
    }

    private void broadcastAccept(MessageServiceOuterClass.ClientRequest request, long seqNum) {
//        logger.info("========== broadcastAccept called for seq={} ==========", seqNum);
//
//        // Debug executor state BEFORE attempting broadcast
//        debugNetworkExecutor();
//
//        // Run a quick test
//        testNetworkExecutorWithLogging();

        // Verify we're still the leader before broadcasting
        if (!state.isLeader()) {
            logger.warn("Not leader anymore, aborting broadcast for seq={}", seqNum);
            return;
        }

        // Check if network executor is operational
        if (networkExecutor.isShutdown()) {
            logger.error("Network executor is shut down! Cannot broadcast ACCEPT for seq={}", seqNum);
            return;
        }
        if (networkExecutor.isTerminated()) {
            logger.error("Network executor is terminated! Cannot broadcast ACCEPT for seq={}", seqNum);
            return;
        }

        MessageServiceOuterClass.AcceptMessage acceptMessage = MessageServiceOuterClass.AcceptMessage.newBuilder().setRequest(request).setBallot(state.getBallot().toProtoBallot()).setSequenceNumber(seqNum).build();

        logger.info("Broadcasting ACCEPT to {} servers for seq={}", otherServerIds.size(), seqNum);

        // Try direct execution in current thread as a test
//        logger.info("TEST: Attempting direct (non-executor) ACCEPT send to first server...");
//        if (!otherServerIds.isEmpty()) {
//            String firstServer = otherServerIds.iterator().next();
//            try {
//                logger.info("Direct send to {} starting...", firstServer);
//                MessageServiceOuterClass.AcceptedMessage directResult = comms.sendAccept(firstServer, acceptMessage);
//                logger.info("Direct send to {} completed! Result: {}", firstServer, directResult != null ? "SUCCESS" : "NULL");
//            } catch (Exception e) {
//                logger.error("Direct send to {} failed: {}", firstServer, e.getMessage(), e);
//            }
//        }

        int submittedCount = 0;
        for (String targetServerId : otherServerIds) {
            final String serverIdCapture = targetServerId;
            final long seqCapture = seqNum;

            try {
//                logger.info("About to submit task for server {}", targetServerId);

                Future<?> future = networkExecutor.submit(() -> {
//                    logger.info(">>>>> TASK STARTED for server {} seq={} on thread {}",
//                            serverIdCapture, seqCapture, Thread.currentThread().getName());
                    try {
//                        logger.info("Calling comms.sendAccept for server {}", serverIdCapture);
                        MessageServiceOuterClass.AcceptedMessage acceptedMessage =
                                comms.sendAccept(serverIdCapture, acceptMessage);

//                        logger.info("comms.sendAccept returned for server {}", serverIdCapture);

                        if (acceptedMessage != null) {
                            logger.info("Received ACCEPTED from {} for seq={}", serverIdCapture, seqCapture);
                            handleAccepted(acceptedMessage);
                        } else {
                            logger.warn("Received null ACCEPTED from {} for seq={}", serverIdCapture, seqCapture);
                        }
                    } catch (StatusRuntimeException e) {
                        if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED)
                            logger.error("Accept request timed out for server {} for seq={} - it might be inactive",
                                    serverIdCapture, seqCapture);
                    } catch (Exception e) {
                        logger.error("Error in task for server {} seq={}: {}",
                                serverIdCapture, seqCapture, e.getMessage(), e);
                    }
                    logger.info("<<<<< TASK FINISHED for server {} seq={}", serverIdCapture, seqCapture);
                });

                submittedCount++;
//                logger.info("Successfully submitted task {} for server {}, Future: {}",
//                        submittedCount, targetServerId, future.getClass().getSimpleName());

            } catch (RejectedExecutionException e) {
                logger.error("Task REJECTED for server {}: {}", targetServerId, e.getMessage());
            } catch (Exception e) {
                logger.error("Error submitting task for server {}: {}", targetServerId, e.getMessage(), e);
            }
        }
//        logger.info("Submitted {}/{} ACCEPT sends to network executor for seq={}", submittedCount, otherServerIds.size(), seqNum);

        // Debug executor state AFTER submitting
//        logger.info("Executor state after submission:");
//        debugNetworkExecutor();
//
//        logger.info("========== broadcastAccept finished ==========");
    }

    public MessageServiceOuterClass.AcceptedMessage handleAccept(MessageServiceOuterClass.AcceptMessage
                                                                         acceptMessage) {

//        if (state.isLeader() && requestTimer.isRunning()) {
//            logger.info("Live leader received ACCEPT message - ignoring");
//            return null;
//        }

        Ballot acceptBallot = Ballot.fromProtoBallot(acceptMessage.getBallot());
        long seqNum = acceptMessage.getSequenceNumber();

        // Use CompletableFuture to coordinate state and log operations
        CompletableFuture<MessageServiceOuterClass.AcceptedMessage> acceptedMessageFuture = CompletableFuture.supplyAsync(() -> updateBallotIfGreater(acceptBallot), stateExecutor).thenComposeAsync(accepted -> {
            if (!accepted) {
                return null;
            }

            // Apply to log in log executor
            return CompletableFuture.supplyAsync(() -> {
                LogEntry existingEntry = log.getLogEntry(seqNum);

                requestsAccepted.put(getRequestKey(acceptMessage.getRequest()), seqNum);

                if (existingEntry == null) {
//                    Refresh timer, as request is being seen for the first time
                    addToPendingRequests(acceptMessage.getRequest(), null, getRequestKey(acceptMessage.getRequest()));
                    log.setLogEntry(seqNum, acceptBallot, acceptMessage.getRequest());
                    clientState.setTimestamp(acceptMessage.getRequest().getClientId(), acceptMessage.getRequest().getTimestamp());
                    logger.info("Applied accept to log at seq={}", seqNum);

                    // Update client state
                    MessageServiceOuterClass.ClientRequest request = acceptMessage.getRequest();
                    messageExecutor.submit(() -> clientState.setTimestamp(request.getClientId(), request.getTimestamp()));

                } else {
                    logger.debug("Accept already in log at seq={}", seqNum);
                }

                logCommunication(
                        String.format("MESSAGE: <ACCEPTED, <%d, %s>, %d> sent to leader %s", acceptBallot.getTerm(), acceptBallot.getServerId(), seqNum, acceptBallot.getServerId())
                );
                logger.info("MESSAGE: <ACCEPTED {}, {}> sent to leader {}", acceptBallot, seqNum, acceptBallot.getServerId());
                return MessageServiceOuterClass.AcceptedMessage.newBuilder().setBallot(acceptMessage.getBallot()).setSequenceNumber(seqNum).setSenderId(serverId).build();

            }, logExecutor);
        });

        try {
            return acceptedMessageFuture.get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Error processing accept: {}", e.getMessage());
            return null;
        }

    }

    //    Update ballot if incoming is greater, return true if updated, false if rejected
//    safe as it is only called in stateExecutor
    private boolean updateBallotIfGreater(Ballot newBallot) {
        if (newBallot.isGreaterThanOrEqual(state.getBallot())) {

            Ballot currentBallot = state.getBallot();

            logger.info("Is {}, {} greater than {}, {}? {}", newBallot.getTerm(), newBallot.getServerId(), currentBallot.getTerm(), currentBallot.getServerId(), newBallot.isGreaterThan(currentBallot));
            if (newBallot.isGreaterThan(currentBallot)) {
                state.setBallot(newBallot);
            }

            if (!state.isBackup()) {
                transitionToBackup(newBallot.getServerId(), newBallot);
            }

            return true;
        } else {
            logger.info("ACCEPT rejected: ballot {} < current ballot {}", newBallot, state.getBallot());
            return false;
        }
    }

    private void handleAccepted(MessageServiceOuterClass.AcceptedMessage acceptedMessage) {
        Ballot ballot = Ballot.fromProtoBallot(acceptedMessage.getBallot());
        long seqNum = acceptedMessage.getSequenceNumber();

        if (acceptedMessage != null && ballot.equals(state.getBallot())) {
            logCommunication(
                    String.format("MESSAGE: <ACCEPTED, <%d, %s>, %d> received from server %s", ballot.getTerm(), ballot.getServerId(), seqNum, acceptedMessage.getSenderId())
            );
            logger.info("MESSAGE: <ACCEPTED> from {}: seq={}", acceptedMessage.getSenderId(), seqNum);

            logExecutor.submit(() -> {

                LogEntry entry = log.getLogEntry(seqNum);

                if (entry == null) {
                    logger.error("Received ACCEPTED for non-existent log entry at seq={}", seqNum);
                    return;
                }

                // Check if already committed
                if (entry.isCommitted() || entry.isExecuted() || requestsCommitted.containsKey(getRequestKey(entry.getRequest()))) {
                    logger.info("Entry at seq={} already committed or executed, ignoring ACCEPTED", seqNum);
                    return;
                }

                if (entry.isCommitted()) {
                    logger.info("Entry at seq={} already committed, trying to execute", seqNum);
                    executeAllCommittedInOrder();
                    return;
                }

                int newVotes = log.incrementVotes(seqNum);
                if (newVotes >= MAJORITY_COUNT) {
                    logger.info("Majority reached for seq={} - marking as committed", seqNum);
                    log.updateToCommitted(seqNum);

                    // Execute all committed entries in order
                    executeAllCommittedInOrder();

                    broadcastCommit(entry.getRequest(), seqNum);
                }
            });
        }
    }

    private void broadcastCommit(MessageServiceOuterClass.ClientRequest request, long seqNum) {

        MessageServiceOuterClass.CommitMessage commitMessage = MessageServiceOuterClass.CommitMessage.newBuilder().setRequest(request).setBallot(state.getBallot().toProtoBallot()).setSequenceNumber(seqNum).build();

        logger.info("Broadcasting COMMIT to {} servers for seq={}", otherServerIds.size(), seqNum);

        // Try direct execution in current thread as a test
//        logger.info("TEST: Attempting direct (non-executor) ACCEPT send to first server...");
//        if (!otherServerIds.isEmpty()) {
//            String firstServer = otherServerIds.iterator().next();
//            try {
//                logger.info("Direct send to {} starting...", firstServer);
//                comms.sendCommit(firstServer, commitMessage);
//                logger.info("Direct send to {} completed!", firstServer);
//            } catch (Exception e) {
//                logger.error("Direct send to {} failed: {}", firstServer, e.getMessage(), e);
//            }
//        }

        for (String targetServerId : otherServerIds) {
            // Use network executor for I/O-bound operations
            networkExecutor.submit(() -> comms.sendCommit(targetServerId, commitMessage));
        }

        requestsCommitted.put(getRequestKey(request), true);

        // Debug executor state AFTER submitting
//        logger.info("Executor state after submission:");
//        debugNetworkExecutor();

        logger.info("========== broadcastCommit finished ==========");
    }

    public void handleCommitMessage(MessageServiceOuterClass.CommitMessage commitMessage) {
        long seqNum = commitMessage.getSequenceNumber();
        Ballot commitBallot = Ballot.fromProtoBallot(commitMessage.getBallot());

//        if (state.isLeader() && requestTimer.isRunning()) {
//            logger.info("Live leader received COMMIT message - ignoring");
//            return;
//        }

        requestsAccepted.put(getRequestKey(commitMessage.getRequest()), seqNum);

        // Submit to log executor for thread-safe log modification
        logExecutor.submit(() -> {
            LogEntry entry = log.getLogEntry(seqNum);

            if (commitBallot.isGreaterThanOrEqual(state.getBallot())) {

                if (state.isLeader()) transitionToBackup(commitBallot.getServerId(), commitBallot);

                if (entry == null) {
                    logger.warn("Received COMMIT for non-existent log entry at seq={}", seqNum);
                    // Add the entry from the commit message
                    log.setLogEntry(seqNum, commitBallot, commitMessage.getRequest());
                    clientState.setTimestamp(commitMessage.getRequest().getClientId(), commitMessage.getRequest().getTimestamp());
                    logger.info("Added missing log entry at seq={} from COMMIT message", seqNum);
                    entry = log.getLogEntry(seqNum);
                }

                if (entry != null && !entry.isCommitted()) {
                    log.updateToCommitted(seqNum);
                    logger.info("Marked seq={} as committed", seqNum);

                    // Execute all committed entries in order
                    executeAllCommittedInOrder();
                } else if (entry != null) {
                    logger.debug("Entry at seq={} already committed", seqNum);
                }
            }
        });
    }

//    private Boolean attemptExecute(LogEntry currentEntry) {
//        logger.info("Attempting to execute transaction at sequence number {}", currentEntry.getSequenceNumber());
//
//        long currentSeqNum = currentEntry.getSequenceNumber();
//
//        if (currentEntry.isExecuted()) {
//            logger.info("{} already executed", currentSeqNum);
//            return null;
//        }
//
//        if (currentSeqNum == 1) {
//            if (currentEntry.isCommitted()) return executeTransaction(currentEntry.getRequest());
//            else return null;
//        }
//
//        long previousSeqNum = currentSeqNum - 1;
//        if (log.isExecuted(previousSeqNum) && currentEntry.isCommitted()) {
//            boolean result = executeTransaction(currentEntry.getRequest());
//            log.updateToExecuted(currentSeqNum);
//            return result;
//        } else return attemptExecute(log.getLogEntry(previousSeqNum));
//    }

    /**
     * Execute all committed entries in sequential order.
     * This replaces the recursive attemptExecute() method.
     * Called after:
     * - Receiving a commit message
     * - Achieving majority on an accept
     */
    private void executeAllCommittedInOrder() {
        long nextSeq = log.getNextUnexecutedSequence();
        long lastExecuted = nextSeq - 1;

        logger.info("Starting sequential execution from seq={}", nextSeq);

        while (true) {
            LogEntry entry = log.getLogEntry(nextSeq);

            // Stop if we've reached an entry that doesn't exist yet
            if (entry == null) {
                logger.debug("No entry at seq={}, stopping execution", nextSeq);
                // TODO: Implement catchup logic here if needed
                break;
            }

            // Stop if we've reached an uncommitted entry
            if (!entry.isCommitted()) {
                logger.debug("Entry at seq={} not yet committed, stopping execution", nextSeq);
                break;
            }

            // Skip if already executed
            if (entry.isExecuted()) {
                logger.debug("Entry at seq={} already executed, skipping", nextSeq);
                nextSeq++;
                continue;
            }

            if (nextSeq > 1) {
                LogEntry previousEntry = log.getLogEntry(nextSeq - 1);
                if (previousEntry == null || !previousEntry.isExecuted()) {
                    logger.error("Attempting to execute seq={} but predecessor seq={} is not executed. Stopping execution.",
                            nextSeq, nextSeq - 1);
                    break;
                }
            }

            // Execute this entry
            logger.info("Executing entry at seq={}", nextSeq);
            boolean result = executeTransaction(entry.getRequest());

            // Mark as executed in log - this should prevent duplicate execution
            boolean marked = log.updateToExecuted(nextSeq);
            if (!marked) {
                logger.error("Failed to mark seq={} as executed", nextSeq);
                break;
            }

            lastExecuted = nextSeq;

            // Handle client reply if this was a pending request
            String requestKey = Log.getRequestKey(entry.getRequest());
            logger.info("Looking for request {} in pending requests to reply", requestKey);
            if (pendingRequests.containsKey(requestKey)) {
                logger.info("Replying to pending request: {}", requestKey);
                networkExecutor.submit(() -> replyToPendingClientRequest(result, requestKey));
            }

            // Move to next entry
            nextSeq++;
        }

        if (lastExecuted >= log.getNextUnexecutedSequence() - 1) {
            logger.info("Sequential execution complete. Last executed: seq={}", lastExecuted);
        }
    }

    private boolean executeTransaction(MessageServiceOuterClass.ClientRequest request) {
        MessageServiceOuterClass.Transaction transaction = request.getTransaction();
        logger.info("Executing transaction: {} -> {} (amount: {})", transaction.getSender(), transaction.getReceiver(), transaction.getAmount());

        try {
            // Perform the transfer
            boolean result = clientState.transferBalance(transaction);

            if (result) {
                logger.info("Transaction executed successfully");

                // Update client state with this request's timestamp
                clientState.setTimestamp(request.getClientId(), request.getTimestamp());
            } else {
                logger.info("Transaction failed - insufficient funds or invalid accounts");
            }

            // Cache the reply
            MessageServiceOuterClass.ClientReply reply = createReply(request, result);
            replyCache.put(getRequestKey(request), reply);
            logger.info("Added request {} to reply cache", getRequestKey(request));

            return result;

        } catch (Exception e) {
            logger.error("Error executing transaction: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private MessageServiceOuterClass.ClientReply createReply(MessageServiceOuterClass.ClientRequest request,
                                                             boolean result) {
        return MessageServiceOuterClass.ClientReply.newBuilder().setClientId(request.getClientId()).setTimestamp(request.getTimestamp()).setSenderId(serverId).setResult(result).build();
    }

    private void replyToPendingClientRequest(boolean result, String requestKey) {
        PendingRequest pendingRequest = pendingRequests.get(requestKey);

        if (pendingRequest.responseObserver() != null) {
            replyToClientRequest(result, pendingRequest.request(), pendingRequest.responseObserver());
        }

        removeFromPendingRequests(requestKey);
    }

    private void replyToClientRequest(boolean result, MessageServiceOuterClass.
            ClientRequest request, StreamObserver<MessageServiceOuterClass.ClientReply> responseObserver) {
        MessageServiceOuterClass.ClientReply reply = createReply(request, result);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    //    to check if the client request is already in the log - don't think we need this function
    public boolean isTimestampLessThanOrEqualToLog(MessageServiceOuterClass.ClientRequest request) {
        return request.getTimestamp() <= clientState.getTimestamp(request.getClientId());
    }

    //    forward a request to the current leader
    private void forwardRequestToLeader(MessageServiceOuterClass.ClientRequest request, String leaderId) {
        // Use network executor for I/O operation
        networkExecutor.submit(() -> {
            try {
                logger.info("Forwarding request {} to leader {}", getRequestKey(request), leaderId);
                comms.forwardClientRequest(leaderId, request);
            } catch (Exception e) {
                logger.error("Error forwarding request to leader {}: {}", leaderId, e.getMessage());
            }
        });
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
        requestTimer.shutdown();

        Future<?> logSaveFuture = logExecutor.submit(log::save);

        try {
            logSaveFuture.get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Log save interrupted during shutdown: {}", e.getMessage());
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Error saving log during shutdown: {}", e.getMessage());
        } catch (TimeoutException e) {
            logger.error("Timeout saving log during shutdown");
        }

        Future<?> stateSaveFuture = stateExecutor.submit(clientState::save);

        try {
            stateSaveFuture.get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Client state save interrupted during shutdown: {}", e.getMessage());
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Error saving client state during shutdown: {}", e.getMessage());
        } catch (TimeoutException e) {
            logger.error("Timeout saving client state during shutdown");
        }

        shutdownExecutor(networkExecutor, "Network");
        shutdownExecutor(streamingExecutor, "Streaming");
        shutdownExecutor(messageExecutor, "Message");
        shutdownExecutor(logExecutor, "Log");
        shutdownExecutor(stateExecutor, "State");

        comms.shutdown();

        logger.info("ServerNode shutdown complete");
    }

    /**
     * Detailed diagnostic method to inspect executor state and identify blocking issues
     */
    private void debugNetworkExecutor() {
        logger.info("==================== NETWORK EXECUTOR DEBUG ====================");

        if (networkExecutor instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor tpe = (ThreadPoolExecutor) networkExecutor;

            logger.info("Pool Configuration:");
            logger.info("  Core Pool Size: {}", tpe.getCorePoolSize());
            logger.info("  Max Pool Size: {}", tpe.getMaximumPoolSize());
            logger.info("  Current Pool Size: {}", tpe.getPoolSize());
            logger.info("  Active Threads: {}", tpe.getActiveCount());
            logger.info("  Largest Pool Size Ever: {}", tpe.getLargestPoolSize());

            logger.info("Task Statistics:");
            logger.info("  Tasks Submitted (approx): {}", tpe.getTaskCount());
            logger.info("  Tasks Completed: {}", tpe.getCompletedTaskCount());
            logger.info("  Tasks In Queue: {}", tpe.getQueue().size());
            logger.info("  Tasks Running: {}", tpe.getActiveCount());
            logger.info("  Tasks Pending: {}", tpe.getTaskCount() - tpe.getCompletedTaskCount());

            logger.info("Executor State:");
            logger.info("  Is Shutdown: {}", tpe.isShutdown());
            logger.info("  Is Terminating: {}", tpe.isTerminating());
            logger.info("  Is Terminated: {}", tpe.isTerminated());

            // Check if threads are stuck
            if (tpe.getActiveCount() > 0 && tpe.getCompletedTaskCount() == 0) {
                logger.error("PROBLEM: Threads are active but no tasks completing - likely DEADLOCK or BLOCKING!");
            }

            if (!tpe.getQueue().isEmpty() && tpe.getActiveCount() < tpe.getCorePoolSize()) {
                logger.error("PROBLEM: Tasks queued but threads not picking them up - thread creation issue!");
            }

            // Dump thread stack traces
//            dumpNetworkExecutorThreads();
        } else {
            logger.warn("NetworkExecutor is not a ThreadPoolExecutor, limited diagnostics available");
            logger.info("  Type: {}", networkExecutor.getClass().getName());
            logger.info("  Is Shutdown: {}", networkExecutor.isShutdown());
            logger.info("  Is Terminated: {}", networkExecutor.isTerminated());
        }

        logger.info("================================================================");
    }

    /**
     * Dump stack traces of all network executor threads to identify what they're doing
     */
    private void dumpNetworkExecutorThreads() {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);

        logger.info("Network Executor Thread Dump:");
        boolean foundNetworkThreads = false;

        for (ThreadInfo threadInfo : threadInfos) {
            // Look for threads created by our network executor
            if (threadInfo.getThreadName().contains(serverId + "-network-io")) {
                foundNetworkThreads = true;
                logger.info("  Thread: {}", threadInfo.getThreadName());
                logger.info("    State: {}", threadInfo.getThreadState());
                logger.info("    Blocked Time: {} ms", threadInfo.getBlockedTime());
                logger.info("    Blocked Count: {}", threadInfo.getBlockedCount());
                logger.info("    Waited Time: {} ms", threadInfo.getWaitedTime());
                logger.info("    Waited Count: {}", threadInfo.getWaitedCount());

                if (threadInfo.getLockName() != null) {
                    logger.info("    Locked on: {}", threadInfo.getLockName());
                    logger.info("    Lock owner: {}", threadInfo.getLockOwnerName());
                }

                StackTraceElement[] stackTrace = threadInfo.getStackTrace();
                logger.info("    Stack trace (top 10):");
                for (int i = 0; i < Math.min(10, stackTrace.length); i++) {
                    logger.info("      {}", stackTrace[i]);
                }
                logger.info("    ---");
            }
        }

        if (!foundNetworkThreads) {
            logger.error("NO NETWORK EXECUTOR THREADS FOUND! Threads may not have been created!");
        }
    }

    /**
     * Submit a test task that logs what thread it's running on
     */
    private void testNetworkExecutorWithLogging() {
        logger.info("Submitting test task to network executor...");

        try {
            Future<String> future = networkExecutor.submit(() -> {
                String threadName = Thread.currentThread().getName();
                logger.info("TEST TASK EXECUTING on thread: {}", threadName);

                // Try a simple operation
                try {
                    Thread.sleep(100);
                    logger.info("TEST TASK completed sleep on thread: {}", threadName);
                } catch (InterruptedException e) {
                    logger.error("TEST TASK interrupted on thread: {}", threadName);
                    return "INTERRUPTED";
                }

                return "SUCCESS on " + threadName;
            });

            logger.info("Test task submitted, waiting for result...");

            try {
                String result = future.get(2, TimeUnit.SECONDS);
                logger.info("Test task result: {}", result);
            } catch (TimeoutException e) {
                logger.error("TEST TASK TIMED OUT - executor threads are not processing tasks!");
                debugNetworkExecutor();
            } catch (ExecutionException e) {
                logger.error("TEST TASK threw exception: {}", e.getCause().getMessage(), e.getCause());
            }
        } catch (Exception e) {
            logger.error("Failed to submit test task: {}", e.getMessage(), e);
        }
    }


    private ExecutorService createMonitoredNetworkExecutor() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                Math.max(50, OTHER_SERVER_COUNT * 2),  // core pool size
                Math.max(50, OTHER_SERVER_COUNT * 2),  // max pool size
                60L, TimeUnit.SECONDS,                  // keep alive time
                new LinkedBlockingQueue<>(),            // work queue
                createNamedThreadFactory("network-io")
        ) {
            @Override
            protected void beforeExecute(Thread t, Runnable r) {
                super.beforeExecute(t, r);
                logger.info("EXECUTOR: Task starting on thread {}", t.getName());
            }

            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);
                if (t != null) {
                    logger.error("EXECUTOR: Task failed with exception", t);
                } else {
                    logger.info("EXECUTOR: Task completed successfully");
                }
            }

            @Override
            protected void terminated() {
                super.terminated();
                logger.info("EXECUTOR: ThreadPoolExecutor terminated");
            }
        };

        return executor;
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

    public ClientState getClientState() {
        return clientState;
    }
}