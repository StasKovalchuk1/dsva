package node;

import message.Message;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;

public class Node {
    private final String nodeId;
    private final int port;
    private final List<InetSocketAddress> otherNodesAddresses;
    private ServerSocket serverSocket;
    private int logicalClock;
    private final Set<String> repliedNodes;
    private final PriorityBlockingQueue<Request> requestQueue;
    private boolean requestingCS;
    private boolean inCS;
    private String sharedVariable;

    // to save connection to other nodes: nodeId -> ObjectOutputStream
    private final Map<String, ObjectOutputStream> outputStreams;
    private final Map<String, Boolean> deferredReplies;

    private final Set<InetSocketAddress> connectedAddresses;

    public Node(String nodeId, int port, List<InetSocketAddress> otherNodesAddresses) {
        this.nodeId = nodeId;
        this.port = port;
        this.otherNodesAddresses = otherNodesAddresses;
        this.logicalClock = 0;
        this.repliedNodes = new HashSet<>();
        this.requestQueue = new PriorityBlockingQueue<>();
        this.requestingCS = false;
        this.inCS = false;
        this.sharedVariable = "Test value";
        this.outputStreams = new HashMap<>();
        this.deferredReplies = new HashMap<>();
        this.connectedAddresses = new HashSet<>();
    }

    public void start() throws IOException {
        // turn on node to start receiving connections
        serverSocket = new ServerSocket(port);
        System.out.println("Node " + nodeId + " started on port " + port);

        // start tread to accept connections
        new Thread(new ServerHandler()).start();

        // connect to other nodes with retries
        new Thread(this::connectToOtherNodes).start();

        // start user interface
        handleUserInput();
    }

    private void connectToOtherNodes() {
        while (outputStreams.size() < otherNodesAddresses.size()) {
            for (InetSocketAddress address : otherNodesAddresses) {
                if (!connectedAddresses.contains(address)) {
                    try {
                        Socket socket = new Socket();
                        socket.connect(address, 2000);
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                        // node id sharing
                        out.writeObject(new Message(Message.MessageType.NODE_ID, logicalClock, nodeId));
                        out.flush();

                        Message idMessage = (Message) in.readObject();
                        if (idMessage.getType() == Message.MessageType.NODE_ID) {
                            String remoteNodeId = idMessage.getSenderId();
                            synchronized (this) {
                                outputStreams.put(remoteNodeId, out);
                            }
                            connectedAddresses.add(address);
                            System.out.println("Connected to " + remoteNodeId + " at " + address);

                            Message syncRequest = new Message(Message.MessageType.SYNC_REQUEST, logicalClock, nodeId);
                            sendMessage(remoteNodeId, syncRequest);

                            new Thread(new ClientHandler(socket, in, out)).start();
                        } else {
                            System.out.println("Unexpected message type during handshake from " + address);
                            socket.close();
                        }
                    } catch (IOException | ClassNotFoundException e) {
                         System.out.println("Unable to connect to " + address + ", retrying...");
                    }
                }
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized void requestCriticalSection() {
        logicalClock++;
        requestingCS = true;
        Request request = new Request(logicalClock, nodeId);
        requestQueue.add(request);
        repliedNodes.clear();

        Message msg = new Message(Message.MessageType.REQUEST, logicalClock, nodeId);
        broadcast(msg);

        // wait for active nodes to reply
        while (repliedNodes.size() < outputStreams.size()) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // enter critical section
        inCS = true;
        System.out.println("Node " + nodeId + " entered critical section.");
    }

    public synchronized void releaseCriticalSection() {
        inCS = false;
        requestingCS = false;
        requestQueue.poll();

        for (Map.Entry<String, Boolean> entry : deferredReplies.entrySet()) {
            if (entry.getValue()) {
                Message reply = new Message(Message.MessageType.REPLY, logicalClock, nodeId);
                sendMessage(entry.getKey(), reply);
                deferredReplies.put(entry.getKey(), false);
            }
        }

        notifyAll();
        System.out.println("Node " + nodeId + " left critical section.");
    }

    public synchronized void readSharedVariable() {
        System.out.println("Current value of shared variable: " + sharedVariable);
    }

    public synchronized void writeSharedVariable(String value) {
        if (!inCS) {
            System.out.println("You should enter critical section before changing variable.");
            return;
        }
        sharedVariable = value;
        System.out.println("Shared variable changed to: " + sharedVariable);

        Message updateMsg = new Message(Message.MessageType.UPDATE, logicalClock, nodeId, value);
        broadcast(updateMsg);
    }

    private void broadcast(Message msg) {
        System.out.println("Sending message " + msg.getType() + " to all nodes - " + outputStreams.keySet());
        for (Map.Entry<String, ObjectOutputStream> entry : outputStreams.entrySet()) {
            try {
                ObjectOutputStream out = entry.getValue();
                out.writeObject(msg);
                out.flush();
                System.out.println("Message " + msg.getType() + " sent to node " + entry.getKey());
            } catch (IOException e) {
                System.out.println("Error during message sending " + entry.getKey());
                e.printStackTrace();
            }
        }
    }

    private void sendMessage(String receiverId, Message msg) {
        ObjectOutputStream out = outputStreams.get(receiverId);
        if (out != null) {
            try {
                // Thread.sleep(2000);

                out.writeObject(msg);
                out.flush();
                System.out.println("Message " + msg.getType() + " sent to node " + receiverId);
            } catch (IOException e) {
                System.out.println("Error during message sending " + receiverId);
                e.printStackTrace();
            }
        } else {
            System.out.println("No connection with node " + receiverId);
        }
    }

    // Обработка входящих сообщений
    private synchronized void handleMessage(Message msg) {
        logicalClock = Math.max(logicalClock, msg.getTimestamp()) + 1;
        switch (msg.getType()) {
            case REQUEST:
                System.out.println("Received REQUEST from " + msg.getSenderId());
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
                } else {
                    deferredReplies.put(msg.getSenderId(), true);
                }
                break;
            case REPLY:
                repliedNodes.add(msg.getSenderId());
                System.out.println("Received REPLY from " + msg.getSenderId());
                if (repliedNodes.size() == outputStreams.size()) {
                    notifyAll();
                }
                break;
            case UPDATE:
                System.out.println("Received UPDATE from " + msg.getSenderId());
                sharedVariable = msg.getUpdatedValue();
                System.out.println("Node " + msg.getSenderId() + " updated shared variable to: " + sharedVariable);
                break;
            case SYNC_REQUEST:
                // Обработка запроса синхронизации
                System.out.println("Получен SYNC_REQUEST от " + msg.getSenderId());
                Message syncResponse = new Message(Message.MessageType.SYNC_RESPONSE, logicalClock, nodeId, sharedVariable);
                sendMessage(msg.getSenderId(), syncResponse);
                break;
            case SYNC_RESPONSE:
                // Обработка ответа на запрос синхронизации
                System.out.println("Получен SYNC_RESPONSE от " + msg.getSenderId() + ": " + msg.getUpdatedValue());
                sharedVariable = msg.getUpdatedValue();
                break;
            default:
                break;
        }
    }

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

    private class ServerHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                    ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());

                    // reading node_id message from the connected node
                    Message idMessage = (Message) in.readObject();
                    if (idMessage.getType() == Message.MessageType.NODE_ID) {
                        String remoteNodeId = idMessage.getSenderId();

                        // send node_id
                        out.writeObject(new Message(Message.MessageType.NODE_ID, logicalClock, nodeId));
                        out.flush();

                        // saving the output stream for this node
                        synchronized (Node.this) {
                            outputStreams.put(remoteNodeId, out);
                        }

                        System.out.println("Received NODE_ID from " + remoteNodeId);

                        new Thread(new ClientHandler(clientSocket, in, out)).start();
                    } else {
                        System.out.println("Unexpected message type during handshake from " + clientSocket.getRemoteSocketAddress());
                        clientSocket.close();
                    }
                } catch (IOException | ClassNotFoundException e) {
                    System.out.println("Error accepting connection.");
                    e.printStackTrace();
                }
            }
        }
    }

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
                while (true) {
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
                        outputStreams.remove(disconnectedNodeId);
                        System.out.println("Connection with node " + disconnectedNodeId + " has been terminated.");
                    }
                }

                // removing the address from connectedAddresses
                synchronized (connectedAddresses) {
                    connectedAddresses.removeIf(address -> {
                        try {
                            return address.getHostName().equals(socket.getInetAddress().getHostName()) &&
                                    address.getPort() == socket.getPort();
                        } catch (Exception ex) {
                            return false;
                        }
                    });
                }

                try {
                    socket.close();
                } catch (IOException exception) {
                    exception.printStackTrace();
                }
            }
        }
    }

    private void handleUserInput() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Enter command: read, write <value>, request, release, exit");
            String command = scanner.nextLine();
            if (command.equalsIgnoreCase("read")) {
                readSharedVariable();
            } else if (command.startsWith("write")) {
                String[] parts = command.split(" ", 2);
                if (parts.length == 2) {
                    writeSharedVariable(parts[1]);
                } else {
                    System.out.println("Invalid command format. Use: write <value>");
                }
            } else if (command.equalsIgnoreCase("request")) {
                new Thread(this::requestCriticalSection).start();
            } else if (command.equalsIgnoreCase("release")) {
                releaseCriticalSection();
            } else if (command.equalsIgnoreCase("exit")) {
                System.out.println("Shutting down node...");
                System.exit(0);
            } else {
                System.out.println("Unknown command.");
            }
        }
    }
}