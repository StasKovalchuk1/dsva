package node;

import io.javalin.Javalin;
import io.javalin.http.Context;
import message.Message;

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
    private final String nodeId;
    private final int port;
    private final List<InetSocketAddress> initialOtherNodesAddresses; // Изначальные узлы для подключения
    private ServerSocket serverSocket;
    private volatile int logicalClock;
    private final Set<String> repliedNodes;
    private final PriorityBlockingQueue<Request> requestQueue;
    private volatile boolean requestingCS;
    private volatile boolean inCS;
    private volatile String sharedVariable;

    // Сохранение соединений с другими узлами: nodeId -> ObjectOutputStream
    private final ConcurrentHashMap<String, ObjectOutputStream> outputStreams = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> deferredReplies = new ConcurrentHashMap<>();

    private final Set<InetSocketAddress> connectedAddresses = Collections.synchronizedSet(new HashSet<>());

    private Javalin app;

    private volatile boolean alive; // Флаг активности узла

    // Для отслеживания известных узлов для восстановления соединений
    private final List<InetSocketAddress> knownNodesAddresses;
    private final ConcurrentHashMap<String, InetSocketAddress> nodeIdToAddressMap = new ConcurrentHashMap<>();

    private volatile int sendDelayMs = 0; // Задержка в мс

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
        // Запуск серверного сокета для приема соединений от других узлов
        serverSocket = new ServerSocket(port);
        System.out.println("Node " + nodeId + " started on port " + port);

        // Запуск потока для обработки входящих соединений
        new Thread(new ServerHandler()).start();

        // Запуск потока для подключения к изначальным узлам
        new Thread(this::connectToInitialOtherNodes).start();

        // Инициализация и настройка Javalin
        setupJavalin();
    }

    private void setupJavalin() {
        app = Javalin.create(config -> {
            // Нет необходимости устанавливать defaultContentType здесь
        }).start(port + 1000); // Используем порт + 1000 для Javalin, чтобы избежать конфликтов

        // Установка типа контента для всех запросов
        app.before(ctx -> ctx.contentType("application/json"));

        // Определение REST-эндпоинтов
        app.get("/read", this::handleRead);
        app.post("/write", this::handleWrite);
        app.post("/request", this::handleRequestCS);
        app.post("/release", this::handleReleaseCS);
        app.post("/join", this::handleJoin);
        app.post("/leave", this::handleLeave);
        app.post("/kill", this::handleKill);
        app.post("/revive", this::handleRevive);
        app.post("/delay", this::handleSetDelay); // Новый эндпоинт
        app.get("/delay", this::handleGetDelay);    // Новый эндпоинт (опционально)
        app.get("/status", this::handleStatus);
        app.post("/shutdown", this::handleShutdown);

        System.out.println("Javalin server started on port " + (port + 1000));
    }

    // Эндпоинт для чтения общей переменной
    private void handleRead(Context ctx) {
        ctx.json(Collections.singletonMap("sharedVariable", sharedVariable));
    }

    // Эндпоинт для записи в общую переменную
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
            ctx.json(Collections.singletonMap("status", "Shared variable updated."));
        } else {
            ctx.status(403).json(Collections.singletonMap("error", "Node is not in critical section."));
        }
    }

    // Эндпоинт для запроса входа в критическую секцию
    private void handleRequestCS(Context ctx) {
        new Thread(this::requestCriticalSection).start();
        ctx.json(Collections.singletonMap("status", "Critical section requested."));
    }

    // Эндпоинт для выхода из критической секции
    private void handleReleaseCS(Context ctx) {
        releaseCriticalSection();
        ctx.json(Collections.singletonMap("status", "Critical section released."));
    }

    // Эндпоинт для подключения к новым узлам
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
            knownNodesAddresses.add(address); // Добавляем в список известных узлов для восстановления
        }
        new Thread(() -> connectToNodes(newNodes)).start();
        ctx.json(Collections.singletonMap("status", "Join initiated for nodes: " + nodesToJoin));
    }

    // Эндпоинт для корректного отключения от узлов
    private void handleLeave(Context ctx) {
        // Если текущий узел находится в критической секции, освобождаем её
        if (inCS) {
            releaseCriticalSection();
        }

        // Получаем копию всех подключённых узлов для безопасной итерации
        Set<String> nodesToLeave = new HashSet<>(outputStreams.keySet());

        List<String> successfullyLeft = new ArrayList<>();
        List<String> failedToLeave = new ArrayList<>();

        for (String nodeIdToLeave : nodesToLeave) {
            if (outputStreams.containsKey(nodeIdToLeave)) {
                // Создаём сообщение LEAVE
                Message leaveMsg = new Message(Message.MessageType.LEAVE, getAndIncrementLogicalClock(), this.nodeId);
                sendMessage(nodeIdToLeave, leaveMsg);

                // Закрываем соединение и удаляем узел из структур данных
                try {
                    ObjectOutputStream out = outputStreams.get(nodeIdToLeave);
                    if (out != null) {
                        out.close(); // Закрываем поток
                    }
                    removeNode(nodeIdToLeave); // Удаляем узел из всех структур данных
                    successfullyLeft.add(nodeIdToLeave);
                    System.out.println("Successfully left node " + nodeIdToLeave);
                } catch (IOException e) {
                    failedToLeave.add(nodeIdToLeave);
                    System.out.println("Failed to leave node " + nodeIdToLeave);
                    e.printStackTrace();
                }
            } else {
                failedToLeave.add(nodeIdToLeave);
                System.out.println("Node " + nodeIdToLeave + " is not connected.");
            }
        }

        ctx.json(Map.of(
                "status", "Leave operation completed.",
                "successfullyLeft", successfullyLeft,
                "failedToLeave", failedToLeave
        ));
    }

    // Эндпоинт для некорректного отключения (симуляция отказа узла)
    private void handleKill(Context ctx) {
        if (!alive) {
            ctx.status(400).json(Collections.singletonMap("error", "Node is already killed."));
            return;
        }
        alive = false;
        // Закрываем все соединения без отправки сообщений LEAVE
        for (String nodeId : new ArrayList<>(outputStreams.keySet())) {
            try {
                ObjectOutputStream out = outputStreams.get(nodeId);
                if (out != null) {
                    out.close();
                }
                // Удаляем адрес из connectedAddresses и nodeIdToAddressMap
                InetSocketAddress address = findAddressByNodeId(nodeId);
                if (address != null) {
                    connectedAddresses.remove(address);
                    nodeIdToAddressMap.remove(nodeId);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        outputStreams.clear();
        connectedAddresses.clear();
        nodeIdToAddressMap.clear();
        ctx.json(Collections.singletonMap("status", "Node has been killed. All connections closed."));
    }

    // Эндпоинт для восстановления коммуникации после kill
    private void handleRevive(Context ctx) {
        if (alive) {
            ctx.status(400).json(Collections.singletonMap("error", "Node is already alive."));
            return;
        }
        alive = true;
        inCS = false;
        requestingCS = false;
        System.out.println("Node " + nodeId + " is being revived. Reconnecting to known nodes.");

        // Очистка connectedAddresses перед попыткой подключения
        connectedAddresses.clear();
        connectToNodes(knownNodesAddresses);

        ctx.json(Collections.singletonMap("status", "Node has been revived. Reconnecting to known nodes."));
    }

    // Эндпоинт для установки задержки в отправке сообщений
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
        ctx.json(Collections.singletonMap("status", "Send delay set to " + delay + "ms."));
    }

    // Эндпоинт для получения текущей задержки
    private void handleGetDelay(Context ctx) {
        ctx.json(Collections.singletonMap("delay", sendDelayMs));
    }

    // Эндпоинт для получения статуса узла
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

    // Эндпоинт для корректного завершения работы узла
    private void handleShutdown(Context ctx) {
        System.out.println("Shutting down node...");
        app.stop();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            serverSocket.close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    // Метод для подключения к изначальным узлам
    private void connectToInitialOtherNodes() {
        connectToNodes(initialOtherNodesAddresses);
    }

    // Метод для подключения к списку узлов
    private void connectToNodes(List<InetSocketAddress> nodes) {
        for (InetSocketAddress address : nodes) {
            if (connectedAddresses.contains(address)) {
                System.out.println("Already connected to " + address);
                continue; // Уже подключены
            }
            try {
                System.out.println("Attempting to connect to " + address);
                Socket socket = new Socket();
                socket.connect(address, 2000);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                // Отправляем сообщение NODE_ID
                out.writeObject(new Message(Message.MessageType.NODE_ID, getAndIncrementLogicalClock(), nodeId, port));
                out.flush();

                // Читаем сообщение NODE_ID от удаленного узла
                Message idMessage = (Message) in.readObject();
                if (idMessage.getType() == Message.MessageType.NODE_ID) {
                    String remoteNodeId = idMessage.getSenderId();
                    synchronized (this) {
                        outputStreams.put(remoteNodeId, out);
                        nodeIdToAddressMap.put(remoteNodeId, address); // Добавляем в map
                    }
                    connectedAddresses.add(address);
                    System.out.println("Successfully connected to " + remoteNodeId + " at " + address);

                    // Отправляем SYNC_REQUEST
                    Message syncRequest = new Message(Message.MessageType.SYNC_REQUEST, getAndIncrementLogicalClock(), nodeId);
                    sendMessage(remoteNodeId, syncRequest);

                    // Запускаем обработчик клиента
                    new Thread(new ClientHandler(socket, in, out)).start();
                } else {
                    System.out.println("Unexpected message type during handshake from " + address);
                    socket.close();
                }
            } catch (IOException | ClassNotFoundException e) {
                System.out.println("Unable to connect to " + address + ". Error: " + e.getMessage());
                // Можно добавить логику повторных попыток подключения
            }
        }
    }

    // Метод для записи в общую переменную
    private synchronized boolean writeSharedVariable(String value) {
        if (!inCS) {
            System.out.println("You should enter critical section before changing variable.");
            return false;
        }
        sharedVariable = value;
        System.out.println("Shared variable changed to: " + sharedVariable);

        Message updateMsg = new Message(Message.MessageType.UPDATE, getAndIncrementLogicalClock(), nodeId, value);
        broadcast(updateMsg);
        return true;
    }

    // Метод для запроса входа в критическую секцию
    public synchronized void requestCriticalSection() {
        requestingCS = true;
        Request request = new Request(getAndIncrementLogicalClock(), nodeId);
        requestQueue.add(request);
        repliedNodes.clear();

        Message msg = new Message(Message.MessageType.REQUEST, logicalClock, nodeId);
        broadcast(msg);

        // Ожидание ответов от всех других узлов
        while (repliedNodes.size() < outputStreams.size()) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Вход в критическую секцию
        inCS = true;
        System.out.println("Node " + nodeId + " entered critical section.");
    }

    // Метод для выхода из критической секции
    public synchronized void releaseCriticalSection() {
        if (!inCS) {
            System.out.println("Node " + nodeId + " is not in critical section.");
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
            }
        }

        notifyAll();
        System.out.println("Node " + nodeId + " left critical section.");
    }

    // Метод для чтения общей переменной (в консоли)
    public synchronized void readSharedVariable() {
        System.out.println("Current value of shared variable: " + sharedVariable);
    }

    // Метод для отправки сообщения всем узлам
    private void broadcast(Message msg) {
        if (!alive) {
            System.out.println("Node is killed. Cannot send messages.");
            return;
        }
        System.out.println("Sending message " + msg.getType() + " to all nodes - " + outputStreams.keySet());
        for (Map.Entry<String, ObjectOutputStream> entry : outputStreams.entrySet()) {
            String receiverId = entry.getKey();
            ObjectOutputStream out = entry.getValue();
            scheduler.schedule(() -> {
                try {
                    out.writeObject(msg);
                    out.flush();
                    System.out.println("Message " + msg.getType() + " sent to node " + receiverId + " with delay " + sendDelayMs + "ms");
                } catch (IOException e) {
                    System.out.println("Error during message sending to " + receiverId);
                    e.printStackTrace();
                }
            }, sendDelayMs, TimeUnit.MILLISECONDS);
        }
    }

    // Метод для отправки сообщения конкретному узлу
    private void sendMessage(String receiverId, Message msg) {
        if (!alive) {
            System.out.println("Node is killed. Cannot send messages.");
            return;
        }
        ObjectOutputStream out = outputStreams.get(receiverId);
        if (out != null) {
            scheduler.schedule(() -> {
                try {
                    out.writeObject(msg);
                    out.flush();
                    System.out.println("Message " + msg.getType() + " sent to node " + receiverId + " with delay " + sendDelayMs + "ms");
                } catch (IOException e) {
                    System.out.println("Error during message sending to " + receiverId);
                    e.printStackTrace();
                }
            }, sendDelayMs, TimeUnit.MILLISECONDS);
        } else {
            System.out.println("No connection with node " + receiverId);
        }
    }

    private synchronized void removeNode(String nodeId) {
        ObjectOutputStream out = outputStreams.get(nodeId);
        if (out != null) {
            try {
                out.close();
                System.out.println("Closed ObjectOutputStream for node " + nodeId);
            } catch (IOException e) {
                System.out.println("Error closing ObjectOutputStream for node " + nodeId);
                e.printStackTrace();
            }
            outputStreams.remove(nodeId);
        }

        InetSocketAddress address = findAddressByNodeId(nodeId);
        if (address != null) {
            connectedAddresses.remove(address);
            nodeIdToAddressMap.remove(nodeId);
            System.out.println("Removed address " + address + " for node " + nodeId);
        }

        System.out.println("Node " + nodeId + " has been removed from connections.");

        // Уведомляем все ожидающие потоки о изменении состояния
        notifyAll();
    }

    // Метод для обработки входящих сообщений
    private synchronized void handleMessage(Message msg) {
        if (!alive) {
            System.out.println("Node is killed. Ignoring incoming message.");
            return;
        }
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
                    Message reply = new Message(Message.MessageType.REPLY, getAndIncrementLogicalClock(), nodeId);
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
                System.out.println("Received SYNC_REQUEST from " + msg.getSenderId());
                Message syncResponse = new Message(Message.MessageType.SYNC_RESPONSE, getAndIncrementLogicalClock(), nodeId, sharedVariable);
                sendMessage(msg.getSenderId(), syncResponse);
                break;
            case SYNC_RESPONSE:
                // Обработка ответа на запрос синхронизации
                System.out.println("Received SYNC_RESPONSE from " + msg.getSenderId() + ": " + msg.getUpdatedValue());
                sharedVariable = msg.getUpdatedValue();
                break;
            case LEAVE:
                // Обработка сообщения LEAVE
                System.out.println("Received LEAVE from " + msg.getSenderId());
                if (outputStreams.containsKey(msg.getSenderId())) {
                    removeNode(msg.getSenderId());
                    System.out.println("Disconnected from node " + msg.getSenderId());
                }
                break;
            default:
                break;
        }
    }

    // Внутренний класс для представления запроса на критическую секцию
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

    // Внутренний класс для обработки входящих соединений
    private class ServerHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                    ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());

                    // Чтение сообщения NODE_ID от подключенного узла
                    Message idMessage = (Message) in.readObject();
                    if (idMessage.getType() == Message.MessageType.NODE_ID) {
                        String remoteNodeId = idMessage.getSenderId();
                        int remoteMainPort = idMessage.getMainPort();

                        // Отправка собственного NODE_ID
                        out.writeObject(new Message(Message.MessageType.NODE_ID, getAndIncrementLogicalClock(), nodeId));
                        out.flush();

                        if (!outputStreams.containsKey(remoteNodeId)) {
                            // Добавление соединения
                            outputStreams.put(remoteNodeId, out);
                            InetSocketAddress mainAddress = new InetSocketAddress("localhost", remoteMainPort);
                            nodeIdToAddressMap.put(remoteNodeId, mainAddress);
                            connectedAddresses.add(mainAddress);
                            System.out.println("ServerHandler:: Received NODE_ID from " + remoteNodeId + " at " + mainAddress);

                            // Отправка SYNC_REQUEST
                            Message syncRequest = new Message(Message.MessageType.SYNC_REQUEST, getAndIncrementLogicalClock(), nodeId);
                            sendMessage(remoteNodeId, syncRequest);
                            System.out.println("ServerHandler:: Sent SYNC_REQUEST to " + remoteNodeId);

                            // Запуск обработчика клиента
                            new Thread(new ClientHandler(clientSocket, in, out)).start();
                        } else {
                            // Соединение уже существует, закрываем новое входящее соединение
                            System.out.println("ServerHandler:: Node " + remoteNodeId + " is already connected. Closing new connection.");
                            clientSocket.close();
                        }
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

    // Внутренний класс для обработки сообщений от конкретного узла
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
                while (alive) { // Обработка сообщений только если узел жив
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
                    }
                }

                try {
                    socket.close();
                } catch (IOException exception) {
                    exception.printStackTrace();
                }
            }
        }
    }

    // Метод для установки задержки в отправке сообщений
    public synchronized void setSendDelay(int delayMs) {
        this.sendDelayMs = delayMs;
        System.out.println("Send delay set to " + delayMs + "ms.");
    }

    // Метод для поиска адреса по nodeId
    private InetSocketAddress findAddressByNodeId(String nodeId) {
        return nodeIdToAddressMap.get(nodeId);
    }

    // Метод для получения и инкрементации логического времени
    private synchronized int getAndIncrementLogicalClock() {
        logicalClock++;
        return logicalClock;
    }
}