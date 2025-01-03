package node;

import io.javalin.Javalin;
import io.javalin.http.Context;
import message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Node {
    private static final Logger logger = LoggerFactory.getLogger(Node.class);

    private final String nodeId;
    private final int port;
    private final List<InetSocketAddress> initialOtherNodesAddresses; // Initial nodes for connection
    private ServerSocket serverSocket;
    private volatile int logicalClock;
    private final Set<String> repliedNodes;
    private final PriorityBlockingQueue<Request> requestQueue;
    private volatile boolean requestingCS;
    private volatile boolean inCS;
    private volatile String sharedVariable;

    // Maintaining connections with other nodes: nodeId -> ObjectOutputStream
    private final ConcurrentHashMap<String, ObjectOutputStream> outputStreams = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> deferredReplies = new ConcurrentHashMap<>();

    private final Set<InetSocketAddress> connectedAddresses = Collections.synchronizedSet(new HashSet<>());

    private Javalin app;

    private volatile boolean alive; // Node activity flag

    // Tracking known nodes for connection restoration
    private final List<InetSocketAddress> knownNodesAddresses;
    private final ConcurrentHashMap<String, InetSocketAddress> nodeIdToAddressMap = new ConcurrentHashMap<>();

    private volatile int sendDelayMs = 0; // Delay in ms

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

    public Node(String nodeId, int port, List<InetSocketAddress> otherNodesAddresses) {
        this.nodeId = nodeId;
        this.port = port;
        this.initialOtherNodesAddresses = new ArrayList<>(otherNodesAddresses);
        this.logicalClock = 0;
        this.repliedNodes = Collections.synchronizedSet(new HashSet<>());
        this.requestQueue = new PriorityBlockingQueue<>();
        this.requestingCS = false;
        this.inCS = false;
        this.sharedVariable = "Test value";
        this.alive = true;
        this.knownNodesAddresses = Collections.synchronizedList(new ArrayList<>(otherNodesAddresses));
    }

    public void start() throws IOException {
        // Start the server socket to accept connections from other nodes
        serverSocket = new ServerSocket(port);
        logger.info("[LogicalClock:{}] Node {} started on port {}", logicalClock, nodeId, port);

        // Start thread to handle incoming connections
        new Thread(new ServerHandler()).start();

        // Start thread to connect to initial nodes
        new Thread(this::connectToInitialOtherNodes).start();

        // Initialize and configure Javalin
        setupJavalin();
    }

    private void setupJavalin() {
        app = Javalin.create(config -> {
            // No need to set defaultContentType here
        }).start(port + 1000); // Use port + 1000 for Javalin to avoid conflicts

        // Set content type for all requests
        app.before(ctx -> ctx.contentType("application/json"));

        // Define REST endpoints
        app.get("/read", this::handleRead);
        app.post("/write", this::handleWrite);
        app.post("/request", this::handleRequestCS);
        app.post("/release", this::handleReleaseCS);
        app.post("/join", this::handleJoin);
        app.post("/leave", this::handleLeave);
        app.post("/kill", this::handleKill);
        app.post("/revive", this::handleRevive);
        app.post("/delay", this::handleSetDelay); // New endpoint
        app.get("/delay", this::handleGetDelay);    // New endpoint (optional)
        app.get("/status", this::handleStatus);
        app.post("/shutdown", this::handleShutdown);

        logger.info("[LogicalClock:{}] Javalin server started on port {}", logicalClock, port + 1000);
    }

    // Endpoint to read the shared variable
    private void handleRead(Context ctx) {
        ctx.json(Collections.singletonMap("sharedVariable", sharedVariable));
    }

    // Endpoint to write to the shared variable
    private void handleWrite(Context ctx) {
        Map<String, String> body;
        try {
            body = ctx.bodyAsClass(Map.class);
        } catch (Exception e) {
            ctx.status(400).json(Collections.singletonMap("error", "Invalid JSON format."));
            return;
        }

        String value = body.get("value");
        if (value == null) {
            ctx.status(400).json(Collections.singletonMap("error", "Missing 'value' in request body."));
            return;
        }
        boolean success = writeSharedVariable(value);
        if (success) {
            logger.info("[LogicalClock:{}] Shared variable updated to: {}", logicalClock, value);
            ctx.json(Collections.singletonMap("status", "Shared variable updated."));
        } else {
            logger.warn("[LogicalClock:{}] Attempted to write shared variable without being in critical section.", logicalClock);
            ctx.status(403).json(Collections.singletonMap("error", "Node is not in critical section."));
        }
    }

    // Endpoint to request entry into the critical section
    private void handleRequestCS(Context ctx) {
        new Thread(this::requestCriticalSection).start();
        ctx.json(Collections.singletonMap("status", "Critical section requested."));
    }

    // Endpoint to release the critical section
    private void handleReleaseCS(Context ctx) {
        releaseCriticalSection();
        ctx.json(Collections.singletonMap("status", "Critical section released."));
    }

    // Endpoint to join new nodes
    private void handleJoin(Context ctx) {
        List<String> nodesToJoin;
        try {
            nodesToJoin = ctx.bodyAsClass(List.class);
        } catch (Exception e) {
            ctx.status(400).json(Collections.singletonMap("error", "Invalid JSON format."));
            return;
        }

        List<InetSocketAddress> newNodes = new ArrayList<>();
        for (String node : nodesToJoin) {
            String[] parts = node.split(":");
            if (parts.length != 2) {
                ctx.status(400).json(Collections.singletonMap("error", "Invalid node format: " + node));
                return;
            }
            String host = parts[0];
            int port;
            try {
                port = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                ctx.status(400).json(Collections.singletonMap("error", "Invalid port for node: " + node));
                return;
            }
            InetSocketAddress address = new InetSocketAddress(host, port);
            newNodes.add(address);
            knownNodesAddresses.add(address); // Add to known nodes list for restoration
        }
        new Thread(() -> connectToNodes(newNodes)).start();
        logger.info("[LogicalClock:{}] Join initiated for nodes: {}", logicalClock, nodesToJoin);
        ctx.json(Collections.singletonMap("status", "Join initiated for nodes: " + nodesToJoin));
    }

    // Endpoint to gracefully leave all nodes
    private void handleLeave(Context ctx) {
        // If the current node is in the critical section, release it
        if (inCS) {
            releaseCriticalSection();
        }

        // Get a copy of all connected nodes for safe iteration
        Set<String> nodesToLeave = new HashSet<>(outputStreams.keySet());

        List<String> successfullyLeft = new ArrayList<>();
        List<String> failedToLeave = new ArrayList<>();

        for (String nodeIdToLeave : nodesToLeave) {
            if (outputStreams.containsKey(nodeIdToLeave)) {
                // Create a LEAVE message
                Message leaveMsg = new Message(Message.MessageType.LEAVE, getAndIncrementLogicalClock(), this.nodeId);
                sendMessage(nodeIdToLeave, leaveMsg);

                // Close the connection and remove the node from data structures
                try {
                    ObjectOutputStream out = outputStreams.get(nodeIdToLeave);
                    if (out != null) {
                        out.close(); // Close the stream
                    }
                    removeNode(nodeIdToLeave); // Remove the node from all data structures
                    successfullyLeft.add(nodeIdToLeave);
                    logger.info("[LogicalClock:{}] Successfully left node {}", logicalClock, nodeIdToLeave);
                } catch (IOException e) {
                    failedToLeave.add(nodeIdToLeave);
                    logger.error("[LogicalClock:{}] Failed to leave node {}", logicalClock, nodeIdToLeave, e);
                }
            } else {
                failedToLeave.add(nodeIdToLeave);
                logger.warn("[LogicalClock:{}] Node {} is not connected.", logicalClock, nodeIdToLeave);
            }
        }

        // Send a response to the client with the results of the operation
        ctx.json(Map.of(
                "status", "Leave operation completed.",
                "successfullyLeft", successfullyLeft,
                "failedToLeave", failedToLeave
        ));
    }

    // Endpoint to simulate an unexpected node failure (kill)
    private void handleKill(Context ctx) {
        if (!alive) {
            ctx.status(400).json(Collections.singletonMap("error", "Node is already killed."));
            return;
        }
        alive = false;
        // Close all connections without sending LEAVE messages
        for (String nodeId : new ArrayList<>(outputStreams.keySet())) {
            try {
                ObjectOutputStream out = outputStreams.get(nodeId);
                if (out != null) {
                    out.close();
                }
                // Remove the node from connectedAddresses and nodeIdToAddressMap
                InetSocketAddress address = findAddressByNodeId(nodeId);
                if (address != null) {
                    connectedAddresses.remove(address);
                    nodeIdToAddressMap.remove(nodeId);
                }
                logger.info("[LogicalClock:{}] Node {} has been killed and disconnected.", logicalClock, nodeId);
            } catch (IOException e) {
                logger.error("[LogicalClock:{}] Error killing node {}", logicalClock, nodeId, e);
            }
        }
        outputStreams.clear();
        connectedAddresses.clear();
        nodeIdToAddressMap.clear();
        ctx.json(Collections.singletonMap("status", "Node has been killed. All connections closed."));
    }

    // Endpoint to revive the node after a kill
    private void handleRevive(Context ctx) {
        if (alive) {
            ctx.status(400).json(Collections.singletonMap("error", "Node is already alive."));
            return;
        }
        alive = true;
        inCS = false;
        requestingCS = false;
        logger.info("[LogicalClock:{}] Node {} is being revived. Reconnecting to known nodes.", logicalClock, nodeId);

        // Clear connectedAddresses before attempting to connect
        connectedAddresses.clear();
        connectToNodes(knownNodesAddresses);

        ctx.json(Collections.singletonMap("status", "Node has been revived. Reconnecting to known nodes."));
    }

    // Endpoint to set the message sending delay
    private void handleSetDelay(Context ctx) {
        Map<String, Integer> body;
        try {
            body = ctx.bodyAsClass(Map.class);
        } catch (Exception e) {
            ctx.status(400).json(Collections.singletonMap("error", "Invalid JSON format."));
            return;
        }

        Integer delay = body.get("delay");
        if (delay == null || delay < 0) {
            ctx.status(400).json(Collections.singletonMap("error", "Invalid delay value. Must be a non-negative integer."));
            return;
        }

        setSendDelay(delay);
        logger.info("[LogicalClock:{}] Send delay set to {}ms.", logicalClock, delay);
        ctx.json(Collections.singletonMap("status", "Send delay set to " + delay + "ms."));
    }

    // Endpoint to get the current message sending delay
    private void handleGetDelay(Context ctx) {
        ctx.json(Collections.singletonMap("delay", sendDelayMs));
    }

    // Endpoint to get the status of the node
    private void handleStatus(Context ctx) {
        Map<String, Object> status = new HashMap<>();
        status.put("nodeId", nodeId);
        status.put("port", port);
        status.put("logicalClock", logicalClock);
        status.put("inCriticalSection", inCS);
        status.put("sharedVariable", sharedVariable);
        status.put("alive", alive);
        status.put("connectedNodes", outputStreams.keySet());
        status.put("connectedAddresses", connectedAddresses);
        status.put("sendDelayMs", sendDelayMs);
        status.put("requestingCS", requestingCS);
        ctx.json(status);
    }

    // Endpoint to gracefully shut down the node
    private void handleShutdown(Context ctx) {
        logger.info("[LogicalClock:{}] Shutting down node {}...", logicalClock, nodeId);
        app.stop();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            serverSocket.close();
        } catch (IOException | InterruptedException e) {
            logger.error("[LogicalClock:{}] Error during shutdown of node {}", logicalClock, nodeId, e);
        }
        System.exit(0);
    }

    // Method to connect to the initial nodes
    private void connectToInitialOtherNodes() {
        connectToNodes(initialOtherNodesAddresses);
    }

    // Method to connect to a list of nodes
    private void connectToNodes(List<InetSocketAddress> nodes) {
        for (InetSocketAddress address : nodes) {
            if (connectedAddresses.contains(address)) {
                logger.info("[LogicalClock:{}] Already connected to {}", logicalClock, address);
                continue; // Already connected
            }
            try {
                logger.info("[LogicalClock:{}] Attempting to connect to {}", logicalClock, address);
                Socket socket = new Socket();
                socket.connect(address, 2000);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                // Send NODE_ID message
                Message nodeIdMsg = new Message(Message.MessageType.NODE_ID, getAndIncrementLogicalClock(), nodeId, port);
                out.writeObject(nodeIdMsg);
                out.flush();

                // Read NODE_ID message from the remote node
                Message idMessage = (Message) in.readObject();
                if (idMessage.getType() == Message.MessageType.NODE_ID) {
                    String remoteNodeId = idMessage.getSenderId();
                    synchronized (this) {
                        outputStreams.put(remoteNodeId, out);
                        nodeIdToAddressMap.put(remoteNodeId, address); // Add to map
                    }
                    connectedAddresses.add(address);
                    logger.info("[LogicalClock:{}] Successfully connected to {} at {}", logicalClock, remoteNodeId, address);

                    // Send SYNC_REQUEST
                    Message syncRequest = new Message(Message.MessageType.SYNC_REQUEST, getAndIncrementLogicalClock(), nodeId);
                    sendMessage(remoteNodeId, syncRequest);

                    // Start client handler
                    new Thread(new ClientHandler(socket, in, out)).start();
                } else {
                    logger.warn("[LogicalClock:{}] Unexpected message type during handshake from {}", logicalClock, address);
                    socket.close();
                }
            } catch (IOException | ClassNotFoundException e) {
                logger.error("[LogicalClock:{}] Unable to connect to {}. Error: {}", logicalClock, address, e.getMessage());
                // Optionally add retry logic here
            }
        }
    }

    // Method to write to the shared variable
    private synchronized boolean writeSharedVariable(String value) {
        if (!inCS) {
            logger.warn("[LogicalClock:{}] Attempted to change shared variable without entering critical section.", logicalClock);
            return false;
        }
        sharedVariable = value;
        logger.info("[LogicalClock:{}] Shared variable changed to: {}", logicalClock, sharedVariable);

        Message updateMsg = new Message(Message.MessageType.UPDATE, getAndIncrementLogicalClock(), nodeId, value);
        broadcast(updateMsg);
        return true;
    }

    // Method to request entry into the critical section
    public synchronized void requestCriticalSection() {
        requestingCS = true;
        Request request = new Request(getAndIncrementLogicalClock(), nodeId);
        requestQueue.add(request);
        repliedNodes.clear();

        Message msg = new Message(Message.MessageType.REQUEST, logicalClock, nodeId);
        broadcast(msg);
        logger.info("[LogicalClock:{}] Broadcasted REQUEST message for critical section.", logicalClock);

        // Wait for replies from all other nodes
        while (repliedNodes.size() < outputStreams.size()) {
            try {
                wait();
            } catch (InterruptedException e) {
                logger.error("[LogicalClock:{}] Interrupted while waiting for REPLY messages.", logicalClock, e);
            }
        }

        // Enter the critical section
        inCS = true;
        logger.info("[LogicalClock:{}] Node {} entered critical section.", logicalClock, nodeId);
    }

    // Method to release the critical section
    public synchronized void releaseCriticalSection() {
        if (!inCS) {
            logger.warn("[LogicalClock:{}] Node {} is not in critical section.", logicalClock, nodeId);
            return;
        }

        inCS = false;
        requestingCS = false;
        requestQueue.poll();

        for (Map.Entry<String, Boolean> entry : deferredReplies.entrySet()) {
            if (entry.getValue()) {
                Message reply = new Message(Message.MessageType.REPLY, getAndIncrementLogicalClock(), nodeId);
                sendMessage(entry.getKey(), reply);
                deferredReplies.put(entry.getKey(), false);
                logger.info("[LogicalClock:{}] Sent deferred REPLY to {}", logicalClock, entry.getKey());
            }
        }

        notifyAll();
        logger.info("[LogicalClock:{}] Node {} left critical section.", logicalClock, nodeId);
    }

    // Method to read the shared variable (logged)
    public synchronized void readSharedVariable() {
        logger.info("[LogicalClock:{}] Current value of shared variable: {}", logicalClock, sharedVariable);
    }

    // Method to broadcast a message to all nodes
    private void broadcast(Message msg) {
        if (!alive) {
            logger.warn("[LogicalClock:{}]:: broadcast:: Node {} is killed. Cannot send messages.", logicalClock, nodeId);
            return;
        }
        // Increment logicalClock before sending the message
        logger.info("[LogicalClock:{}] Sending message {} to all nodes - {}", logicalClock, msg.getType(), outputStreams.keySet());
        for (Map.Entry<String, ObjectOutputStream> entry : outputStreams.entrySet()) {
            String receiverId = entry.getKey();
            ObjectOutputStream out = entry.getValue();
            scheduler.schedule(() -> {
                try {
                    logger.info("[LogicalClock:{}]:: broadcast:: Message {} sent to node {} with delay {}ms", logicalClock, msg.getType(), receiverId, sendDelayMs);
                    out.writeObject(msg);
                    out.flush();
                } catch (IOException e) {
                    logger.error("[LogicalClock:{}] Error during message sending to {}", logicalClock, receiverId, e);
                }
            }, sendDelayMs, TimeUnit.MILLISECONDS);
        }
    }

    // Method to send a message to a specific node
    private void sendMessage(String receiverId, Message msg) {
        if (!alive) {
            logger.warn("[LogicalClock:{}]:: sendMessage:: Node {} is killed. Cannot send messages.", logicalClock, nodeId);
            return;
        }
        ObjectOutputStream out = outputStreams.get(receiverId);
        if (out != null) {
            scheduler.schedule(() -> {
                try {
                    // Increment logicalClock before sending the message
                    logger.info("[LogicalClock:{}]:: sendMessage:: Message {} sent to node {} with delay {}ms", logicalClock, msg.getType(), receiverId, sendDelayMs);
                    out.writeObject(msg);
                    out.flush();
                } catch (IOException e) {
                    logger.error("[LogicalClock:{}] Error during message sending to {}", logicalClock, receiverId, e);
                }
            }, sendDelayMs, TimeUnit.MILLISECONDS);
        } else {
            logger.warn("[LogicalClock:{}] No connection with node {}", logicalClock, receiverId);
        }
    }

    // Synchronized method to remove a node and notify waiting threads
    private synchronized void removeNode(String nodeId) {
        ObjectOutputStream out = outputStreams.get(nodeId);
        if (out != null) {
            try {
                out.close();
                logger.info("[LogicalClock:{}] Closed ObjectOutputStream for node {}", logicalClock, nodeId);
            } catch (IOException e) {
                logger.error("[LogicalClock:{}] Error closing ObjectOutputStream for node {}", logicalClock, nodeId, e);
            }
            outputStreams.remove(nodeId);
        }

        InetSocketAddress address = findAddressByNodeId(nodeId);
        if (address != null) {
            connectedAddresses.remove(address);
            nodeIdToAddressMap.remove(nodeId);
            logger.info("[LogicalClock:{}] Removed address {} for node {}", logicalClock, address, nodeId);
        }

        logger.info("[LogicalClock:{}] Node {} has been removed from connections.", logicalClock, nodeId);

        // Notify all waiting threads about the change in state
        notifyAll();
    }

    // Method to handle incoming messages
    private synchronized void handleMessage(Message msg) {
        if (!alive) {
            logger.warn("[LogicalClock:{}] Node {} is killed. Ignoring incoming message.", logicalClock, nodeId);
            return;
        }
        int previousClock = logicalClock;
        logicalClock = Math.max(logicalClock, msg.getTimestamp()) + 1;
        switch (msg.getType()) {
            case REQUEST:
                logger.info("[LogicalClock:{}] Received REQUEST from {}", logicalClock, msg.getSenderId());
                Request incomingRequest = new Request(msg.getTimestamp(), msg.getSenderId());
                requestQueue.add(incomingRequest);

                boolean shouldReply = false;

                if (!requestingCS) {
                    shouldReply = true;
                } else {
                    Request currentRequest = requestQueue.peek();
                    if (currentRequest != null && incomingRequest.compareTo(currentRequest) < 0) {
                        shouldReply = true;
                    }
                }

                if (shouldReply) {
                    Message reply = new Message(Message.MessageType.REPLY, logicalClock, nodeId);
                    sendMessage(msg.getSenderId(), reply);
                    logger.info("[LogicalClock:{}] Sent REPLY to {}", logicalClock, msg.getSenderId());
                } else {
                    deferredReplies.put(msg.getSenderId(), true);
                    logger.info("[LogicalClock:{}] Deferred REPLY to {}", logicalClock, msg.getSenderId());
                }
                break;
            case REPLY:
                repliedNodes.add(msg.getSenderId());
                logger.info("[LogicalClock:{}] Received REPLY from {}", logicalClock, msg.getSenderId());
                if (repliedNodes.size() == outputStreams.size()) {
                    notifyAll();
                    logger.info("[LogicalClock:{}] All REPLY messages received. Notifying waiting thread.", logicalClock);
                }
                break;
            case UPDATE:
                logger.info("[LogicalClock:{}] Received UPDATE from {}", logicalClock, msg.getSenderId());
                sharedVariable = msg.getUpdatedValue();
                logger.info("[LogicalClock:{}] Node {} updated shared variable to: {}", logicalClock, msg.getSenderId(), sharedVariable);
                break;
            case SYNC_REQUEST:
                // Handle synchronization request
                logger.info("[LogicalClock:{}] Received SYNC_REQUEST from {}", logicalClock, msg.getSenderId());
                Message syncResponse = new Message(Message.MessageType.SYNC_RESPONSE, logicalClock, nodeId, sharedVariable);
                sendMessage(msg.getSenderId(), syncResponse);
                logger.info("[LogicalClock:{}] Sent SYNC_RESPONSE to {}", logicalClock, msg.getSenderId());
                break;
            case SYNC_RESPONSE:
                // Handle synchronization response
                logger.info("[LogicalClock:{}] Received SYNC_RESPONSE from {}: {}", logicalClock, msg.getSenderId(), msg.getUpdatedValue());
                if (msg.getTimestamp() > previousClock) {
                    // Incoming update is more recent
                    sharedVariable = msg.getUpdatedValue();
                    logger.info("[LogicalClock:{}] Node {} synced shared variable to: {}", logicalClock, msg.getSenderId(), sharedVariable);
                } else {
                    // Incoming update is outdated; ignore
                    logger.info("[LogicalClock:{}] Received outdated SYNC_RESPONSE from {}. Ignoring.", logicalClock, msg.getSenderId());
                }
                break;
            case LEAVE:
                // Handle LEAVE message
                logger.info("[LogicalClock:{}] Received LEAVE from {}", logicalClock, msg.getSenderId());
                if (outputStreams.containsKey(msg.getSenderId())) {
                    removeNode(msg.getSenderId());
                    logger.info("[LogicalClock:{}] Disconnected from node {}", logicalClock, msg.getSenderId());
                }
                break;
            default:
                logger.warn("[LogicalClock:{}] Received unknown message type: {}", logicalClock, msg.getType());
                break;
        }
    }

    // Inner class to represent a request for the critical section
    private static class Request implements Comparable<Request>, Serializable {
        private final int timestamp;
        private final String nodeId;

        public Request(int timestamp, String nodeId) {
            this.timestamp = timestamp;
            this.nodeId = nodeId;
        }

        @Override
        public int compareTo(Request o) {
            if (this.timestamp != o.timestamp) {
                return Integer.compare(this.timestamp, o.timestamp);
            }
            return this.nodeId.compareTo(o.nodeId);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Request other) {
                return this.timestamp == other.timestamp && this.nodeId.equals(other.nodeId);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, nodeId);
        }
    }

    // Inner class to handle incoming connections
    private class ServerHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                    ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());

                    // Read NODE_ID message from the connected node
                    Message idMessage = (Message) in.readObject();
                    if (idMessage.getType() == Message.MessageType.NODE_ID) {
                        String remoteNodeId = idMessage.getSenderId();
                        int remoteMainPort = idMessage.getMainPort();

                        // Send own NODE_ID message with the main port
                        Message ownNodeIdMsg = new Message(Message.MessageType.NODE_ID, getAndIncrementLogicalClock(), nodeId, port);
                        out.writeObject(ownNodeIdMsg);
                        out.flush();
                        logger.info("[LogicalClock:{}] Sent NODE_ID to {}", logicalClock, clientSocket.getRemoteSocketAddress());

                        if (!outputStreams.containsKey(remoteNodeId)) {
                            // Add the connection
                            outputStreams.put(remoteNodeId, out);
                            InetSocketAddress mainAddress = new InetSocketAddress(clientSocket.getInetAddress().getHostAddress(), remoteMainPort);
                            nodeIdToAddressMap.put(remoteNodeId, mainAddress);
                            connectedAddresses.add(mainAddress);
                            logger.info("[LogicalClock:{}] Received NODE_ID from {} at {}", logicalClock, remoteNodeId, mainAddress);

                            // Start client handler
                            new Thread(new ClientHandler(clientSocket, in, out)).start();
                        } else {
                            // Connection already exists, close the new incoming connection
                            logger.warn("[LogicalClock:{}] Node {} is already connected. Closing new connection.", logicalClock, remoteNodeId);
                            clientSocket.close();
                        }
                    } else {
                        logger.warn("[LogicalClock:{}] Unexpected message type during handshake from {}", logicalClock, clientSocket.getRemoteSocketAddress());
                        clientSocket.close();
                    }
                } catch (IOException | ClassNotFoundException e) {
                    logger.error("[LogicalClock:{}] Error accepting connection.", logicalClock, e);
                }
            }
        }
    }

    // Inner class to handle messages from a specific node
    private class ClientHandler implements Runnable {
        private final Socket socket;
        private final ObjectInputStream in;
        private final ObjectOutputStream out;

        public ClientHandler(Socket socket, ObjectInputStream in, ObjectOutputStream out) {
            this.socket = socket;
            this.in = in;
            this.out = out;
        }

        @Override
        public void run() {
            try {
                while (alive) { // Handle messages only if the node is alive
                    Message msg = (Message) in.readObject();
                    handleMessage(msg);
                }
            } catch (IOException | ClassNotFoundException e) {
                String disconnectedNodeId = null;
                synchronized (Node.this) {
                    for (Map.Entry<String, ObjectOutputStream> entry : outputStreams.entrySet()) {
                        if (entry.getValue() == out) {
                            disconnectedNodeId = entry.getKey();
                            break;
                        }
                    }
                    if (disconnectedNodeId != null) {
                        removeNode(disconnectedNodeId);
                        logger.info("[LogicalClock:{}] Connection with node {} has been terminated.", logicalClock, disconnectedNodeId);
                    }
                }

                try {
                    socket.close();
                } catch (IOException exception) {
                    logger.error("[LogicalClock:{}] Error closing socket for node {}", logicalClock, disconnectedNodeId, exception);
                }
            }
        }
    }

    // Method to set the message sending delay
    public synchronized void setSendDelay(int delayMs) {
        this.sendDelayMs = delayMs;
        logger.info("[LogicalClock:{}] Send delay set to {}ms.", logicalClock, delayMs);
    }

    // Method to find the address by nodeId
    private InetSocketAddress findAddressByNodeId(String nodeId) {
        return nodeIdToAddressMap.get(nodeId);
    }

    // Synchronized method to get and increment the logical clock
    private synchronized int getAndIncrementLogicalClock() {
        logicalClock++;
        return logicalClock;
    }
}
