/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.performance;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.utils.CmdUtils;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class RemoteTest {

    public static final int MAX_WORKERS = 8;
    public static Map<Integer, VectorClock> vectorClockMap = new HashMap<Integer, VectorClock>();

    public static class KeyProvider {

        private static final int MILLION = 1 * 1000 * 1000;
        private final List<String> keys;
        private final AtomicInteger index;

        public KeyProvider(int start, List<String> keys) {
            this.index = new AtomicInteger(start);
            this.keys = keys;
        }

        public Integer next() {
            if(keys != null) {
                return Integer.parseInt(keys.get(index.getAndIncrement() % keys.size()));
            } else {
                return MILLION + (int) (Math.random() * (50 * MILLION));
            }
        }
    }

    public static void printUsage(PrintStream out, OptionParser parser) throws IOException {
        out.println("Usage: $VOLDEMORT_HOME/bin/remote-test.sh \\");
        out.println("          [options] bootstrapUrl storeName num-requests\n");
        parser.printHelpOn(out);
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {

        OptionParser parser = new OptionParser();
        parser.accepts("r", "execute read operations");
        parser.accepts("w", "execute write operations");
        parser.accepts("d", "execute delete operations");
        parser.accepts("request-file", "execute specific requests in order")
              .withRequiredArg()
              .ofType(String.class);
        parser.accepts("start-key-index", "starting point when using int keys. Default = 0")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("value-size", "size in bytes for random value.  Default = 1024")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("iterations", "number of times to repeat the test  Default = 1")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("threads", "max number concurrent worker threads  Default = " + MAX_WORKERS)
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("saveValidKeys", "should we save all valid keys in a file Default = false")
              .withRequiredArg()
              .ofType(Boolean.class);

        OptionSet options = parser.parse(args);

        List<String> nonOptions = options.nonOptionArguments();
        if(nonOptions.size() != 3) {
            printUsage(System.err, parser);
        }

        String url = nonOptions.get(0);
        String storeName = nonOptions.get(1);
        int numRequests = Integer.parseInt(nonOptions.get(2));
        String ops = "";
        List<String> keys = null;

        Integer startNum = CmdUtils.valueOf(options, "start-key-index", 0);
        Integer valueSize = CmdUtils.valueOf(options, "value-size", 1024);
        Integer numIterations = CmdUtils.valueOf(options, "iterations", 1);
        Integer numThreads = CmdUtils.valueOf(options, "threads", MAX_WORKERS);
        final Boolean saveValidKeys = CmdUtils.valueOf(options, "saveValidKeys", false);

        final DataOutputStream dos = (saveValidKeys) ? new DataOutputStream(new FileOutputStream(new File((String) options.valueOf("request-file"))))
                                                    : null;
        if(!saveValidKeys && options.has("request-file")) {
            keys = loadKeys((String) options.valueOf("request-file"));
        }

        if(options.has("r")) {
            ops += "r";
        }
        if(options.has("w")) {
            ops += "w";
        }
        if(options.has("d")) {
            ops += "d";
        }
        if(ops.length() == 0) {
            ops = "rwd";
        }

        System.out.println("operations : " + ops);
        System.out.println("value size : " + valueSize);
        System.out.println("start index : " + startNum);
        System.out.println("iterations : " + numIterations);
        System.out.println("threads : " + numThreads);

        System.out.println("Bootstraping cluster data.");
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setMaxThreads(numThreads)
                                                                                    .setMaxTotalConnections(numThreads)
                                                                                    .setMaxConnectionsPerNode(numThreads)
                                                                                    .setBootstrapUrls(url)
                                                                                    .setConnectionTimeout(60,
                                                                                                          TimeUnit.SECONDS)
                                                                                    .setSocketTimeout(60,
                                                                                                      TimeUnit.SECONDS)
                                                                                    .setSocketBufferSize(4 * 1024));
        final StoreClient<Integer, byte[]> store = factory.getStoreClient(storeName);
        final byte[] value = TestUtils.randomLetters(valueSize).getBytes();
        ExecutorService service = Executors.newFixedThreadPool(numThreads);

        for(int loopCount = 0; loopCount < numIterations; loopCount++) {

            System.out.println("======================= iteration = " + loopCount
                               + " ======================================");

            if(ops.contains("w")) {
                System.out.println("Beginning write test.");
                final KeyProvider keyProvider1 = new KeyProvider(startNum, keys);
                final CountDownLatch latch1 = new CountDownLatch(numRequests);
                long start = System.currentTimeMillis();
                for(int i = 0; i < numRequests; i++) {
                    service.execute(new Runnable() {

                        public void run() {
                            try {
                                Integer key = keyProvider1.next();
                                store.put(key, value);
                            } catch(Exception e) {
                                e.printStackTrace();
                            } finally {
                                latch1.countDown();
                            }
                        }
                    });
                }
                latch1.await();
                long writeTime = System.currentTimeMillis() - start;
                System.out.println("Throughput: " + (numRequests / (float) writeTime * 1000)
                                   + " writes/sec.");
            }

            if(ops.contains("r")) {
                System.out.println("Beginning read test.");
                final KeyProvider keyProvider2 = new KeyProvider(startNum, keys);
                final CountDownLatch latch2 = new CountDownLatch(numRequests);
                long start = System.currentTimeMillis();
                keyProvider2.next();
                for(int i = 0; i < numRequests; i++) {
                    service.execute(new Runnable() {

                        public void run() {
                            try {
                                Integer key = keyProvider2.next();
                                Versioned<byte[]> v = store.get(key);

                                if(v == null) {
                                    throw new Exception("value returned is null for key " + key);
                                } else {
                                    if(saveValidKeys) {
                                        dos.writeUTF(Integer.toString(key) + "\n");
                                    } else {
                                        // do a put ()
                                        store.put(key, v);
                                        Versioned<byte[]> v2 = store.get(key);
                                        VectorClock oldClock = (VectorClock) v.getVersion();
                                        VectorClock newClock = (VectorClock) v2.getVersion();
                                        if(newClock.compare(oldClock) != Occured.AFTER) {
                                            throw new Exception("stale value returnded for key:"
                                                                + key);
                                        }
                                    }
                                }
                            } catch(Exception e) {
                                e.printStackTrace();
                            } finally {
                                latch2.countDown();
                            }
                        }
                    });
                }
                latch2.await();
                long readTime = System.currentTimeMillis() - start;
                System.out.println("Throughput: " + (numRequests / (float) readTime * 1000.0)
                                   + " reads/sec.");
            }
        }
        System.exit(0);
    }

    public static List<String> loadKeys(String path) throws IOException {

        List<String> targets = new ArrayList<String>();
        File file = new File(path);
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            String text;
            while((text = reader.readLine()) != null) {
                String numberString = text.trim();
                if(numberString.length() > 0)
                    targets.add(numberString);
            }
        } finally {
            try {
                if(reader != null) {
                    reader.close();
                }
            } catch(IOException e) {
                e.printStackTrace();
            }
        }

        return targets;
    }
}
