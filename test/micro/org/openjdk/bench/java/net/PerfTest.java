import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

class Result implements Serializable {
    private final long putCount, getCount;
    private final double totalTime;
    private final double avgGets, avgPuts;

    Result(long putCount, long getCount, double totalTime, double avgGets, double avgPuts) {
        this.putCount = putCount;
        this.getCount = getCount;
        this.totalTime = totalTime;
        this.avgGets = avgGets;
        this.avgPuts = avgPuts;
    }

    public long getPutCount() {
        return putCount;
    }

    public long getGetCount() {
        return getCount;
    }

    public double getTotalTime() {
        return totalTime;
    }

    public double avgGets() {
        return avgGets;
    }

    public double avgPuts() {
        return avgPuts;
    }
}

class Message extends PerfTest implements Serializable, Comparable<Message> {
    private final int port;
    private final Result result;
    private final MessageType type;
    private final InetAddress addr;

    Message(InetAddress addr, int port, MessageType type, Result result) {
        this.port = port;
        this.type = type;
        this.addr = addr;
        this.result = result;
    }

    public InetAddress getAddr() {
        return addr;
    }

    public Result getResult() {
        return result;
    }

    public int getPort() {
        return port;
    }

    public MessageType getType() {
        return type;
    }

    @Override
    public int compareTo(Message other) {
        return this.getAddr().getHostName().compareTo(other.getAddr().getHostName());
    }
}

class PerfTest {
    private static int time;
    private static int threadCount;
    private static int MSG_RECEIVE_PORT, MULTICAST_PORT;
    private static double getPutRatio;
    private String localhostName;
    private static InetAddress LOCAL_SERVER_ADDR, MULTICAST_ADDR;

    private final LongAdder getCount = new LongAdder();
    private final LongAdder putCount = new LongAdder();
    private final LongAdder pktSentCount = new LongAdder();
    private final LongAdder pktReceiveCount = new LongAdder();
    private final static List<String> members = new ArrayList<>();

    public enum MessageType {JOIN, START, CLOSE, RESULT}

    // Initialise settings for test
    public void start() throws IOException, InterruptedException {
        time = 60; // seconds
        threadCount = 25;
        getPutRatio = 0.8; // 80% gets, 20% puts

        MULTICAST_PORT = 4447;
        MULTICAST_ADDR = InetAddress.getByName("224.0.0.2");
        LOCAL_SERVER_ADDR = getLocalAddress();

        AtomicBoolean stopServer = new AtomicBoolean(false);

        //new Thread(() -> new Server(stopServer)).start();
        Server server = new Server(stopServer);
        Thread serverThread = new Thread(server);
        serverThread.start();

        Coordinator coordinator = new Coordinator();
        // Join multicast group
        coordinator.sendToMulticastGroup(MULTICAST_ADDR, null, MessageType.JOIN);
        coordinator.coordinateTasks();
    }

    private InetAddress getLocalAddress() throws SocketException {
        List<InetAddress> addrList = new ArrayList<>();
        Enumeration<NetworkInterface> Interfaces = NetworkInterface.getNetworkInterfaces();
        while (Interfaces.hasMoreElements()) {
            NetworkInterface Interface = Interfaces.nextElement();
            Enumeration<InetAddress> Addresses = Interface.getInetAddresses();
            while (Addresses.hasMoreElements()) {
                InetAddress Address = Addresses.nextElement();
                if (!(Address instanceof Inet6Address))
                    if (!Address.getHostAddress().equals(InetAddress.getLoopbackAddress().getHostAddress())) {
                        addrList.add(Address);
                    }
            }
        }
        return addrList.get(0);
    }

    // Wait for message, send reply
    private class Server implements Runnable {
        AtomicBoolean running;

        Server(AtomicBoolean running) {
            this.running = running;
        }

        public void run() {
            try (var server = new DatagramSocket(0, LOCAL_SERVER_ADDR)) {
                MSG_RECEIVE_PORT = server.getLocalPort();

                while (!running.get()) {
                    DatagramPacket receivePkt = new DatagramPacket(new byte[1024], 1024);
                    server.receive(receivePkt);
                    pktReceiveCount.increment(); // change to local variable, coordinate later

                    // unpack received pkt
                    ByteBuffer bb = ByteBuffer.wrap(receivePkt.getData());
                    int type = bb.getInt();
                    SocketAddress returnAddr = getReturnAddr(bb);

                    switch (type) {
                        case 0: { // GET
                            DatagramPacket getResponse = new DatagramPacket(new byte[1024], 1024, returnAddr);
                            server.send(getResponse);
                            break;
                        }
                        case 1: { // PUT
                            DatagramPacket getResponse = new DatagramPacket(new byte[128], 128, returnAddr);
                            server.send(getResponse);
                            break;
                        }
                    }
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        private SocketAddress getReturnAddr(ByteBuffer bb) throws UnknownHostException {
            byte[] buf = new byte[4];
            bb.get(buf, 0, 4);
            InetAddress returnAddr = InetAddress.getByAddress(buf);
            int port = bb.getInt();

            return new InetSocketAddress(returnAddr, port);
        }
    }

    /**
     * Coordinator handles the coordination of tasks, such as running the performance
     * test and membership of the performance group, simultaneously across all
     * members. It does this by multicasting packets between members.
     */
    private class Coordinator {

        /**
         * Prompt acts as the immediary between user and coordinator and it presents
         * options that can be used to control the starting and stopping of the test.
         * <ul>
         *      <li> Pressing the {@code 'S'} character will start the test for all member
         *      nodes.
         *      <li> Pressing the {@code 'X'} character will close the test on all member
         *      nodes.
         * </ul>
         */
        private class Prompt implements Runnable {
            private AtomicBoolean running;
            private final BufferedReader br;

            Prompt(AtomicBoolean running) {
                this.running = running;
                br = new BufferedReader(new InputStreamReader(System.in));
            }

            // display prompt
            public void run() {
                while (!running.get()) {
                    try {
                        switch (br.readLine()) {
                            case "S": {
                                sendToMulticastGroup(MULTICAST_ADDR, null, MessageType.START);
                                break;
                            }
                            default: {
                                System.out.println("Unknown input");
                                break;
                            }
                        }
                    } catch (NullPointerException | IOException ex) {
                        if(ex.getCause() instanceof InterruptedException)
                            System.out.println("interrupt caught");
                        ex.printStackTrace();
                    }
                }
                System.out.println("Prompt closed");
            }
        }

        private int resultsNotReceived;
        private MulticastSocket multicastSocket;
        private Thread promptThread;
        private Prompt prompt;
        private AtomicBoolean stopPrompt;
        private List<Message> results; // change to Result, how to get name?

        Coordinator() {
            try {
                // set up multicastSocket to coordinate membership
                multicastSocket = new MulticastSocket(MULTICAST_PORT);
                multicastSocket.joinGroup(MULTICAST_ADDR);
                stopPrompt = new AtomicBoolean(false);

                results = new ArrayList<>();

                prompt = new Prompt(stopPrompt);
                promptThread = new Thread(prompt);
                promptThread.start();
                // add local host to member list
                localhostName = String.format("%s:%d", LOCAL_SERVER_ADDR.getHostAddress(), MSG_RECEIVE_PORT);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        /**
         * This method loops and receives incoming packets from other members of
         * the multicast group. Depending on type one of four things can occur:
         * <ul>
         *      <li> JOIN - A membership packet has been received. This indicates
         *      that another member has joined the multicast group. Record the
         *      address of the new member and send the local address of this
         *      instance to them.
         *      <li> START - A start test packet has been received. Start a test
         *      server on this instance and begin the performance test.
         *      <li> RESULT - A packet containing the result of the performance
         *      test on a member has been received. If this is the instance that
         *      initiated the test, compile the result.
         *      <li> CLOSE - A close packet has been received. Close down  a test
         *      server on this instance and begin the performance test.
         * </ul>
         *
         */
        private void coordinateTasks() throws InterruptedException {
            boolean loop = true;

            System.out.println("Please press S to start or X to exit");
            printMemberGroup();
            while (loop) {
                try {
                    byte[] buffer = new byte[1024];
                    DatagramPacket receivePkt = new DatagramPacket(buffer, buffer.length);
                    multicastSocket.receive(receivePkt);

                    ByteArrayInputStream baos = new ByteArrayInputStream(buffer);
                    ObjectInputStream oos = new ObjectInputStream(baos);
                    Message msg = (Message) oos.readObject();

                    MessageType type = msg.getType();
                    int port = msg.getPort();
                    InetAddress returnAddr = msg.getAddr();

                    switch (type) {
                        case JOIN: {
                            updateMemberGroup(returnAddr, port);
                            break;
                        }
                        case START: {
                            Result localResult = runBenchmark();
                            sendToMulticastGroup(returnAddr, localResult, MessageType.RESULT);
                            break;
                        }
                        case RESULT: {
                            results.add(msg);
                            if (resultsNotReceived == 0)
                                printResults();
                            else
                                resultsNotReceived--;
                            break;
                        }
                        default: {
                            System.out.println("pkt has unknown type: " + type);
                            break;
                        }
                    }
                } catch (IOException | ClassNotFoundException ex) {
                    ex.printStackTrace();
                }
            }
            System.out.println("Coordinate loop broken");
        }

        // Update member group and send new member local address
        private void updateMemberGroup(InetAddress addr, int port) {
            String hostName = String.format("%s:%d", addr.getHostAddress(), port);

            // check if member is not current member or already in group
            if ((!localhostName.contains(hostName)) && (!members.contains(hostName))) {
                members.add(hostName);
                sendToMulticastGroup(addr, null, MessageType.JOIN);
                printMemberGroup();
                resultsNotReceived++;
            }
        }

        // Multicast message to members of group
        private void sendToMulticastGroup(InetAddress returnAddr, Result result, MessageType type) {
            Message msg = new Message(LOCAL_SERVER_ADDR, MSG_RECEIVE_PORT, type, result);
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(msg);
                oos.flush();
                // get the byte array of the object
                byte[] buf = baos.toByteArray();
                DatagramPacket sendPkt = new DatagramPacket(buf, buf.length, returnAddr, MULTICAST_PORT);

                multicastSocket.send(sendPkt);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        // Print out members of performance test group
        private void printMemberGroup() {
            StringBuilder memberStr = new StringBuilder(localhostName);
            if (!members.isEmpty()) {
                for (String member : members)
                    memberStr.append(", ").append(member);
            }
            System.out.printf("Member list: [%s]\n", memberStr.toString());
        }

        private String formatInBytes(double bytes) {
            double tmp;

            if (bytes < 1000)
                return String.format("%,.2fb", bytes);
            if (bytes < 1_000_000) {
                tmp = bytes / 1000.0;
                return String.format("%,.2fKB", tmp);
            }
            if (bytes < 1_000_000_000) {
                tmp = bytes / 1000_000.0;
                return String.format("%,.2fMB", tmp);
            } else {
                tmp = bytes / 1_000_000_000.0;
                return String.format("%,.2fGB", tmp);
            }
        }

        // Print compiled total results from all members
        private void printResults() {

            long totalRequests = 0;
            double totalGetCount = 0, totalPutCount = 0;
            double totalTime = 0;
            double totalAvgGetTime = 0, totalAvgPutTime = 0;

            System.out.println("\n======================= Results: ===========================");

            results.sort(Message::compareTo);
            for (Message msg : results) {
                Result result = msg.getResult();

                String resultStr = String.format("%s: %,d reqs/sec (%,d gets, %,d puts, get RTT %,.2f us, put RTT %,.2f us)",
                        msg.getAddr().getHostName(),
                        result.getGetCount()+result.getPutCount(),
                        result.getGetCount(),
                        result.getPutCount(),
                        result.avgGets() / 1000.0,
                        result.avgPuts()/ 1000.0);
                System.out.println(resultStr);

                // compile total times for all members
                totalTime += result.getTotalTime();

                // compile count totals
                totalGetCount += result.getGetCount();
                totalPutCount += result.getPutCount();
                totalRequests += totalGetCount + totalPutCount;
                // compile time totals
                totalAvgGetTime += result.avgGets() / 1000;
                totalAvgPutTime += result.avgPuts() / 1000;
            }

            double totalRequestPerSec = totalRequests / (totalTime / 1000.0);
            double throughput = totalRequestPerSec * 1024; // * msg size
            double finalAvgGet = totalAvgGetTime / results.size();
            double finalAvgPut = totalAvgPutTime / results.size();
            System.out.println();
            System.out.printf("Throughput: %,.2f reqs/sec/node (%s/sec)\n" +
                            "Roundtrip:  gets %,.2f us, puts %,.2f us\n",
                    totalRequestPerSec, formatInBytes(throughput),
                    finalAvgGet, finalAvgPut);
        }
    }


    /**
     * Sender handles the sending of packets. Each sender will
     * use the loopback addr and choose an ephemeral port to perform it's send.
     */
    private class Sender implements Runnable {
        private int port;
        private boolean running; // need to sync this (volitile)

        private DatagramSocket sender;
        private final CountDownLatch latch;
        private final List<Long> getTimes, putTimes;

        //private long avgPutTime, avgGetTime;

        Sender(CountDownLatch latch) {
            running = true;
            getTimes = putTimes = new ArrayList<>();

            this.latch = latch;
            try {
                sender = new DatagramSocket(0);
                port = sender.getLocalPort();
            } catch (SocketException se) {
                se.printStackTrace();
            }
        }

        // Stop sender and close underlying socket
        public void close() {
            running = false;
            sender.close();
        }

        // Begin sending and measure time taken
        public void run() {
            try {
                latch.await(); // wait for all threads before starting
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (running) {
                try {
                    if (isGet()) { // GET

                        DatagramPacket getRequest = buildRequest(0);
                        DatagramPacket getResponse = new DatagramPacket(new byte[1024], 1024);

                        long startTime = System.nanoTime();
                        sender.send(getRequest);
                        sender.receive(getResponse);
                        long getTime = System.nanoTime() - startTime;

                        getCount.increment();
                        getTimes.add(getTime);

                    } else { // PUT

                        DatagramPacket putRequest = buildRequest(1);
                        DatagramPacket putResponse = new DatagramPacket(new byte[128], 128);

                        long startTime = System.nanoTime();
                        sender.send(putRequest);
                        sender.receive(putResponse);
                        long putTime = System.nanoTime() - startTime;
                        putCount.increment();
                        putTimes.add(putTime);
                    }
                    pktSentCount.increment();
                } catch (IOException ex) {
                    if (running) {
                        ex.printStackTrace();
                    }
                }
            }
        }

        // Choose GET or PUT based on ratio prescribed
        private boolean isGet() {
            if (getPutRatio >= 1) return true;
            if (getPutRatio <= 0) return false;
            return (new Random().nextInt(100) < getPutRatio * 100);
        }

        // Build request package to be sent by sender
        private DatagramPacket buildRequest(int type) throws UnknownHostException {
            ByteBuffer bb = ByteBuffer.allocate(128);
            bb.putInt(type);
            byte[] addr = LOCAL_SERVER_ADDR.getAddress();
            bb.put(addr);
            bb.putInt(port);
            byte[] buf = bb.array();
            InetSocketAddress target = getRandomMemberAddress();

            return new DatagramPacket(buf, buf.length, target);
        }

        // Pick a random member of test group to send message to
        private InetSocketAddress getRandomMemberAddress() throws UnknownHostException {
            if (members.isEmpty())
                return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);

            String member = members.get(new Random().nextInt(members.size()));
            int delimiter = member.indexOf(":");
            String host = member.substring(0, delimiter);
            int port = Integer.parseInt(member.substring(delimiter + 1));

            return new InetSocketAddress(InetAddress.getByName(host), port);
        }
    }
    // Print average request time for current member per iteration
    private String printAvgReqTime(long start_time) {
        long duration = System.currentTimeMillis() - start_time;
        long totalGets = getCount.sum();
        long totalPuts = putCount.sum();
        double requestsPerSec = (totalGets + totalPuts) / (duration / 1000.0);

        return String.format("%,.0f reqs/sec (%,d total reads, %,d total writes)", requestsPerSec, totalGets, totalPuts);
    }

    // Run benchmark test for this member
    public Result runBenchmark() throws InterruptedException {
        System.out.printf("\nRun time: %s seconds\n", time);

        getCount.reset();
        putCount.reset();
        pktSentCount.reset();
        pktReceiveCount.reset();

        final CountDownLatch latch = new CountDownLatch(1);
        // thread set up
        Sender[] senders = new Sender[threadCount];
        Thread[] threads = new Thread[threadCount];
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            senders[i] = new Sender(latch);
            threads[i] = executor.getThreadFactory().newThread(senders[i]);
            threads[i].start();
        }

        // start
        long startTime = System.currentTimeMillis();
        latch.countDown(); // senders begin
        long interval = (long) ((time * 1000.0) / 10.0);

        // Perform as many GETs/PUTs as possible in time allotted.
        for (int i = 1; i <= 10; i++) {
            Thread.sleep(interval);
            System.out.printf("%d: %s\n", i, printAvgReqTime(startTime));
        }

        // finish
        for (PerfTest.Sender sender : senders)
            sender.close();
        for (Thread thread : threads)
            thread.join();
        long totalTime = System.currentTimeMillis() - startTime;

        // print totals
        System.out.println();
        System.out.println("Total time taken: " + totalTime + " ms");
        System.out.println("Total requests sent: " + pktSentCount.sum());
        System.out.println("Total requests received: " + pktReceiveCount.sum());

        // compile averages across sender threads
        long avgGetTime = 0, avgPutTime = 0;
        long totalGets = getCount.sum(), totalPuts = putCount.sum();
        for (Sender sender : senders) {
            for (Long getTime : sender.getTimes) {
                avgGetTime += getTime;
            }
            avgGetTime = avgGetTime / totalGets;
            for (Long putTime : sender.putTimes) {
                avgPutTime += putTime;
            }
            avgPutTime = avgPutTime / totalPuts;
        }
        // store results in Result object
        return new Result(totalPuts, totalGets, totalTime, avgGetTime, avgPutTime);
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        PerfTest perfTest = new PerfTest();
        perfTest.start();
    }
}