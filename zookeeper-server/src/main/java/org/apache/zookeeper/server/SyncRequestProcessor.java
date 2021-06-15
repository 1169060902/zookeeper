/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 *
 * SyncRequestProcessor is used in 3 different cases
 * 1. Leader - Sync request to disk and forward it to AckRequestProcessor which
 *             send ack back to itself.
 * 2. Follower - Sync request to disk and forward request to
 *             SendAckRequestProcessor which send the packets to leader.
 *             SendAckRequestProcessor is flushable which allow us to force
 *             push packets to leader.
 * 3. Observer - Sync committed request to disk (received as INFORM packet).
 *             It never send ack back to the leader, so the nextProcessor will
 *             be null. This change the semantic of txnlog on the observer
 *             since it only contains committed txns.
 *
 *  其目的是进行持久化，
 * 也就是将消息存储到磁盘文件中；代码不多，但有不少值得借鉴的地方；
 *
 */
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);
    private final ZooKeeperServer zks;
    //在zookeeper中各个工作责任链之间进行消息通信的是通过LinkedBlockingQueue 来进行线程间信息交互的；
    // queuedRequests就是SyncRequestProcessor和上一责任链之间进行消息交互的队列；
    private final LinkedBlockingQueue<Request> queuedRequests =
            new LinkedBlockingQueue<Request>();
    private final RequestProcessor nextProcessor;

    private Thread snapInProcess = null;
    volatile private boolean running;

    /**
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     * 待flush到磁盘的事务日志消息容器，包括增、删、改消息，查询类消息不进入该容器；
     */
    private final LinkedList<Request> toFlush = new LinkedList<Request>();
    private final Random r = new Random();
    /**
     * The number of log entries to log before starting a snapshot
     * 生成snapshot的事务记录参数值，可在zoo.cfg中进行配置，
     * 即事务日志记录数大于等于snapCount（其具体算法在下面进行探讨，这里先这样记录）的时候，进行snapshot文件的生成；
     */
    private static int snapCount = ZooKeeperServer.getSnapCount();

    private final Request requestOfDeath = Request.requestOfDeath;

    public SyncRequestProcessor(ZooKeeperServer zks,
                                RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks
                .getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        running = true;
    }

    /**
     * used by tests to check for changing
     * snapcounts
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
    }

    /**
     * used by tests to get the snapcount
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }

    /**
     * 当前方法主要时写日志
     * 写事务日志到磁盘
     *
     * https://zhuanlan.zhihu.com/p/73892886
     *
     */
    @Override
    public void run() {
        try {
            //统计条数
            int logCount = 0;

            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time
            int randRoll = r.nextInt(snapCount / 2);
            while (true) {
                Request si = null;
                if (toFlush.isEmpty()) {
                    si = queuedRequests.take();
                } else {
                    si = queuedRequests.poll();
                    if (si == null) {
                        flush(toFlush);
                        continue;
                    }
                }
                if (si == requestOfDeath) {
                    break;
                }
                if (si != null) {
                    // track the number of records written to the log
                    if (zks.getZKDatabase().append(si)) {
                        logCount++;
                        //如果 logCount 统计本次数据大于配置阈值 则触发数据快照
                        if (logCount > (snapCount / 2 + randRoll)) {
                            randRoll = r.nextInt(snapCount / 2);
                            // roll the log
                            zks.getZKDatabase().rollLog();
                            // take a snapshot
                            if (snapInProcess != null && snapInProcess.isAlive()) {
                                LOG.warn("Too busy to snap, skipping");
                            } else {
                                //异步触发 数据快照
                                snapInProcess = new ZooKeeperThread("Snapshot Thread") {
                                    public void run() {
                                        try {
                                            zks.takeSnapshot();
                                        } catch (Exception e) {
                                            LOG.warn("Unexpected exception", e);
                                        }
                                    }
                                };
                                snapInProcess.start();
                            }
                            //统计次数重载
                            logCount = 0;
                        }
                    } else if (toFlush.isEmpty()) {
                        // optimization for read heavy workloads
                        // iff this is a read, and there are no pending
                        // flushes (writes), then just pass this to the next
                        // processor
                        if (nextProcessor != null) {
                            nextProcessor.processRequest(si);
                            if (nextProcessor instanceof Flushable) {
                                ((Flushable) nextProcessor).flush();
                            }
                        }
                        continue;
                    }
                    toFlush.add(si);
                    if (toFlush.size() > 1000) {
                        flush(toFlush);
                    }
                }
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
        } finally {
            running = false;
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    /**
     * 数据日志同步到磁盘
     * @param toFlush
     * @throws IOException
     * @throws RequestProcessorException
     */
    private void flush(LinkedList<Request> toFlush)
            throws IOException, RequestProcessorException {
        if (toFlush.isEmpty())
            return;
        //zks.getZKDatabase().commit(); 消息进行刷盘至磁盘文件中；
        zks.getZKDatabase().commit();
        while (!toFlush.isEmpty()) {
            Request i = toFlush.remove();
            if (nextProcessor != null) {
                //将消息递交下一处理链；
                nextProcessor.processRequest(i);
            }
        }
        if (nextProcessor != null && nextProcessor instanceof Flushable) {
            ((Flushable) nextProcessor).flush();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        queuedRequests.add(requestOfDeath);
        try {
            if (running) {
                this.join();
            }
            if (!toFlush.isEmpty()) {
                flush(toFlush);
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while wating for " + this + " to finish");
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    /**
     * 添加日志 到队列里
     * @param request
     */
    public void processRequest(Request request) {
        // request.addRQRec(">sync");
        queuedRequests.add(request);
    }

}
