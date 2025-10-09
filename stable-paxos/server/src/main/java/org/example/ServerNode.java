package org.example;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;

public class ServerNode {

    private static final Logger logger = LogManager.getLogger(ServerNode.class);

    private final int MAJORITY_COUNT = 2;
    private final int OTHER_SERVER_COUNT = 4;
    private final long WAIT_BEFORE_PREPARE_MIN_MILLIS = 2000;
    private final long WAIT_BEFORE_PREPARE_MAX_MILLIS = 3000;
    private final long REQUEST_TIMEOUT_MILLIS = 5000;

    private final String serverId;
    private final ServerState state;
    private final Set<String> otherServerIds;
    private final Timer timerBeforePrepare;
    private final Timer requestTimer;
    private final ClientState clientState;
    private final Log log;

    // Track pending client requests
    private final ConcurrentHashMap<String, PendingRequest> pendingRequests;

    //    Track received prepare messages
    private final ConcurrentHashMap<String, MessageServiceOuterClass.PrepareMessage> receivedPrepareMessages;

    // Track client requests
    private final ConcurrentHashMap<String, MessageServiceOuterClass.ClientReply> replyCache;

    //    Handle GRPC communications
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
        this.receivedPrepareMessages = new ConcurrentHashMap<>();
        this.replyCache = new ConcurrentHashMap<>();

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
        logger.warn("Request timeout! {} pending CLIENT requests not executed", pendingRequests.size());

        if (pendingRequests.size() > 0 && !state.isLeader()) {
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

    private String getRequestKey(MessageServiceOuterClass.ClientRequest request) {
        return request.getClientId() + "-" + request.getTimestamp();
    }

    // Role changes------------------------------------------------------------------------------------------------------------

    // triggered whenever backup role's timer expires or when leader id is null (on startup)
    public void transitionToCandidate() {
        logger.info("Transitioning to candidate role");

        state.setRole(Role.CANDIDATE);
        state.setLeaderId(null);

        if (requestTimer.isRunning()) requestTimer.stop();

        logger.info("Role: CANDIDATE, Ballot: {}, LeaderId: null", state.getBallot());

        attemptPrepare();
    }

    //    triggered when promiseQueue collects a majority quorum of promises
    public void transitionToLeader() {
        logger.info("Transitioning to leader role");

        state.setRole(Role.LEADER);
        state.setLeaderId(serverId);
        timerBeforePrepare.stop();
//        leaderLivenessTimer.stop();

        logger.info("Role: LEADER, Ballot: {}, LeaderId: {}", state.getBallot(), serverId);

        processAcceptLogFromPromises();

        Ballot ballot = state.getBallot();
        broadcastNewView(ballot);
        processPendingRequests();
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

        for (String serverId : otherServerIds) {
            networkExecutor.submit(() -> {
                try {
                    MessageServiceOuterClass.PromiseMessage promise = comms.sendPrepare(serverId, currentBallot);

                    if (promise != null) {
                        logger.info("MESSAGE: <PROMISE, <{}, {}>, acceptLog ({} items)> received from server {}", promise.getBallot().getInstance(), promise.getBallot().getSenderId(), promise.getAcceptLogList().size(), promise.getSenderId());

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

        for (MessageServiceOuterClass.PromiseMessage promise : promises) {
            for (MessageServiceOuterClass.AcceptMessage acceptMessage : promise.getAcceptLogList()) {
                logExecutor.submit(() -> processOthersAcceptMessage(acceptMessage));
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
            logExecutor.submit(() -> processClientRequest(pendingRequest.request()));
        }

        // Clear all pending requests
        pendingRequests.clear();
        requestTimer.stop();

        logger.info("Finished processing pending client requests");
    }

    //    apply the accept messages from accept log received in promises, to self log
    private void processOthersAcceptMessage(MessageServiceOuterClass.AcceptMessage acceptMessage) {
        long seqNum = acceptMessage.getSequenceNumber();
        Ballot acceptBallot = Ballot.fromProtoBallot(acceptMessage.getBallot());
        LogEntry existingEntry = log.getLogEntry(seqNum);

        if (existingEntry == null) {
            log.add(acceptMessage.getRequest(), acceptBallot);
            logger.info("Applied accept from promise to log at seq={}", seqNum);
        } else if (acceptBallot.isGreaterThan(existingEntry.getBallot())) {
            log.setLogEntry(seqNum, acceptBallot, existingEntry.getRequest());
            logger.info("Updated ballot from accept in promise in log at seq={}", seqNum);
        } else {
            logger.debug("Accept from promise already in log at seq={}", seqNum);
        }

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
        boolean leaderIsAlive = requestTimer.isRunning();

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

//        #TODO: access the streamobserver for highest prepare and send promise

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

    // NORMAL OPERATIONS phase - prepare and promise handling------------------------------------------------------------------

    //    Handle incoming client request - add to pendingRequests and refresh the timer
    public void handleClientRequest(MessageServiceOuterClass.ClientRequest request, StreamObserver<MessageServiceOuterClass.ClientReply> responseObserver) {
        String requestKey = getRequestKey(request);

        // Submit to message executor for processing
        messageExecutor.submit(() -> {
            // Check if request is already executed (reads from clientState)
            logger.info("Is timestamp less than or equal to log? {}", isTimestampLessThanOrEqualToLog(request));
            if (isTimestampLessThanOrEqualToLog(request)) {
                logger.info("Request already executed, ignoring");
//                check in replyCache
                if (replyCache.containsKey(requestKey) && responseObserver != null) {
                    MessageServiceOuterClass.ClientReply reply = replyCache.get(requestKey);
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                }
                return;
            }

            // Check current state and route accordingly
            stateExecutor.submit(() -> {
//                Seeing a client request for the first time - reset timer

                if (state.isLeader() || state.getRole() == null) {
                    logger.info("Processing request immediately");
                    logExecutor.submit(() -> processClientRequest(request));
                }

                String currentLeader = state.getLeaderId();
                if (currentLeader != null && state.isBackup()) {
                    // has to be backup
                    logger.info("Forwarding request to leader: {}", currentLeader);
                    forwardRequestToLeader(request, currentLeader);
                }

//                kick-off leader election phase if we're in the system init phase meaning role will be null
                if (state.getRole() == null) {
                    transitionToCandidate();
                }

                if (pendingRequests.containsKey(requestKey)) {
                    return;
                }

                // Add to pending requests and start timer
                logger.info("Starting / Refreshing leader liveness timer ({}ms)", REQUEST_TIMEOUT_MILLIS);
                requestTimer.startOrRefresh();
                pendingRequests.put(requestKey, new PendingRequest(request, responseObserver));

                logger.info("No leader - added to pending requests. Total pending: {}", pendingRequests.size());
            });

        });
    }

    public void processClientRequest(MessageServiceOuterClass.ClientRequest request) {
//        Add request to log
        logger.info("Processing client request as leader: {}", getRequestKey(request));

        long seqNum = log.add(request, state.getBallot()); // safe as this is called within a stateExecutor
        logger.info("Added to log at sequence: {}", seqNum);

        broadcastAccept(request, seqNum);

//        CompletableFuture.supplyAsync(state::isLeader, stateExecutor).thenAccept((isLeader) -> {
//            if (isLeader) {
//                broadcastAccept(request, seqNum);
//            }
//        });
    }

    public void broadcastNewView(Ballot ballot) {

        CompletableFuture<MessageServiceOuterClass.NewViewMessage> newViewBuilderFuture = CompletableFuture.supplyAsync(() -> MessageServiceOuterClass.NewViewMessage.newBuilder().addAllAcceptLog(log.getAcceptLog()).setBallot(ballot.toProtoBallot()).build(), logExecutor);
        newViewBuilderFuture.thenAccept(newViewMessage -> {
            for (String targetServerId : otherServerIds) {
                comms.sendNewView(targetServerId, newViewMessage, this::handleAccepted);
            }
        });

    }

    public void handleNewView(MessageServiceOuterClass.NewViewMessage newViewMessage, StreamObserver<MessageServiceOuterClass.AcceptedMessage> responseObserver) {

        networkExecutor.submit(() -> {
            for (int i = 0; i < newViewMessage.getAcceptLogCount(); i++) {
                MessageServiceOuterClass.AcceptMessage acceptMessage = newViewMessage.getAcceptLog(i);
                responseObserver.onNext(handleAccept(acceptMessage));
            }

            responseObserver.onCompleted();
        });
    }

    private void broadcastAccept(MessageServiceOuterClass.ClientRequest request, long seqNum) {

        MessageServiceOuterClass.AcceptMessage acceptMessage = MessageServiceOuterClass.AcceptMessage.newBuilder().setRequest(request).setBallot(state.getBallot().toProtoBallot()).setSequenceNumber(seqNum).build();

        for (String targetServerId : otherServerIds) {
            // Use network executor for I/O-bound operations
            networkExecutor.submit(() -> {
                try {
                    MessageServiceOuterClass.AcceptedMessage acceptedMessage = comms.sendAccept(targetServerId, acceptMessage);
                    handleAccepted(acceptedMessage);
                } catch (Exception e) {
                    logger.error("Error sending accept to {}: {}", targetServerId, e.getMessage());
                }
            });
        }
    }

    public MessageServiceOuterClass.AcceptedMessage handleAccept(MessageServiceOuterClass.AcceptMessage acceptMessage) {
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

                if (existingEntry == null) {
//                    Refresh timer, as request is being seen for the first time
                    requestTimer.startOrRefresh();
                    log.add(acceptMessage.getRequest(), acceptBallot);
                    logger.info("Applied accept to log at seq={}", seqNum);

                    // Update client state
                    MessageServiceOuterClass.ClientRequest request = acceptMessage.getRequest();
                    messageExecutor.submit(() -> clientState.setTimestamp(request.getClientId(), request.getTimestamp()));

                } else {
                    logger.debug("Accept already in log at seq={}", seqNum);
                }

                logger.info("MESSAGE: <ACCEPTED> sent to leader {}", acceptBallot.getServerId());
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

            if (newBallot.isGreaterThan(state.getBallot())) {
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
            logger.info("MESSAGE: <ACCEPTED> from {}: seq={}", acceptedMessage.getSenderId(), seqNum);
            // TODO: Track accepted for commit phase
            logExecutor.submit(() -> {

                LogEntry entry = log.getLogEntry(seqNum);

                if (entry == null) {
                    logger.error("Received ACCEPTED for non-existent log entry at seq={}", seqNum);
                    return;
                }

                // Check if already committed
                if (entry.isCommitted()) {
                    logger.debug("Entry at seq={} already committed, ignoring ACCEPTED", seqNum);
                    return;
                }

                int newVotes = log.incrementVotes(seqNum);
                if (newVotes >= MAJORITY_COUNT) {
                    logger.info("Majority reached for seq={} - marking as committed", seqNum);
                    log.updateToCommitted(seqNum);
                    executeTransaction(entry.getRequest());
                    broadcastCommit(entry.getRequest(), seqNum);
                }
            });
        }
    }

    private void broadcastCommit(MessageServiceOuterClass.ClientRequest request, long seqNum) {

        MessageServiceOuterClass.CommitMessage commitMessage = MessageServiceOuterClass.CommitMessage.newBuilder().setRequest(request).setBallot(state.getBallot().toProtoBallot()).setSequenceNumber(seqNum).build();

        for (String targetServerId : otherServerIds) {
            // Use network executor for I/O-bound operations
            networkExecutor.submit(() -> comms.sendCommit(targetServerId, commitMessage));
        }
    }

    public void handleCommitMessage(MessageServiceOuterClass.CommitMessage commitMessage) {
        long seqNum = commitMessage.getSequenceNumber();
        Ballot commitBallot = Ballot.fromProtoBallot(commitMessage.getBallot());

        // Submit to log executor for thread-safe log modification
        logExecutor.submit(() -> {
            LogEntry entry = log.getLogEntry(seqNum);

            if (entry == null) {
                logger.warn("Received COMMIT for non-existent log entry at seq={}", seqNum);
                // Add the entry from the commit message
                log.add(commitMessage.getRequest(), commitBallot);
                entry = log.getLogEntry(seqNum);
            }

            if (entry != null && !entry.isCommitted() && !entry.isExecuted()) {

                log.updateToCommitted(seqNum);
                Boolean result = attemptExecute(entry);

                if (result != null) {
                    logger.info("Committed and executed transaction at seq={}", seqNum);

//                reply to pending requests if needed
                    String requestKey = getRequestKey(commitMessage.getRequest());
                    if (pendingRequests.containsKey(requestKey)) {
                        logger.info("Replying to pending request stored from earlier - {}", requestKey);
                        networkExecutor.submit(() -> replyToPendingClientRequest(result, requestKey));
                    }
                }
            } else if (entry != null) {
                logger.debug("Entry at seq={} already committed", seqNum);
            }
        });
    }

    private Boolean attemptExecute(LogEntry currentEntry) {
        logger.info("Attempting to execute transaction at sequence number {}", currentEntry.getSequenceNumber());

        long currentSeqNum = currentEntry.getSequenceNumber();

        if (currentSeqNum == 1) {
            if (currentEntry.isCommitted()) return executeTransaction(currentEntry.getRequest());
            else return null;
        }

        long previousSeqNum = currentSeqNum - 1;
        if (log.isExecuted(previousSeqNum) && currentEntry.isCommitted()) {
            boolean result = executeTransaction(currentEntry.getRequest());
            log.updateToExecuted(currentSeqNum);
            return result;
        } else return attemptExecute(log.getLogEntry(previousSeqNum));
    }

    private boolean executeTransaction(MessageServiceOuterClass.ClientRequest request) {
        MessageServiceOuterClass.Transaction transaction = request.getTransaction();
        logger.info("Executing transaction: {} -> {} (amount: {})", transaction.getSender(), transaction.getReceiver(), transaction.getAmount());

        try {
            requestTimer.stop();
            // Perform the transfer
            boolean success = clientState.transferBalance(transaction);

            if (success) {
                logger.info("Transaction executed successfully");
            } else {
                logger.info("Transaction failed - insufficient funds or invalid accounts");
            }

            return success;

        } catch (Exception e) {
            logger.error("Error executing transaction: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void replyToPendingClientRequest(boolean result, String requestKey) {
        PendingRequest pendingRequest = pendingRequests.get(requestKey);

        if (pendingRequest.responseObserver() != null) {
            replyToClientRequest(result, pendingRequest.request(), pendingRequest.responseObserver());
        }

//        remove from pendingRequests
        pendingRequests.remove(requestKey);
    }

    private void replyToClientRequest(boolean result, MessageServiceOuterClass.ClientRequest request, StreamObserver<MessageServiceOuterClass.ClientReply> responseObserver) {
        MessageServiceOuterClass.ClientReply reply = MessageServiceOuterClass.ClientReply.newBuilder().setClientId(request.getClientId()).setTimestamp(request.getTimestamp()).setSenderId(serverId).setResult(result).build();

        String requestKey = getRequestKey(request);
        replyCache.put(requestKey, reply);

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
        requestTimer.shutdown();  // Add this

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