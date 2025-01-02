import node.Node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Incorrect input. Use: java -jar Node.jar <nodeId> <port> <otherNode1Host>:<port> <otherNode2Host>:<port> ...");
            System.exit(0);
        }

        String nodeId = args[0];
        int port = Integer.parseInt(args[1]);
        List<InetSocketAddress> otherNodes = new ArrayList<>();

        for (int i = 2; i < args.length; i++) {
            String[] parts = args[i].split(":");
            String host = parts[0];
            int otherPort = Integer.parseInt(parts[1]);
            otherNodes.add(new InetSocketAddress(host, otherPort));
        }

        Node node = new Node(nodeId, port, otherNodes);
        node.start();
    }
}
