/*
 *    Copyright (c) 2020, VRAI Labs and/or its affiliates. All rights reserved.
 *
 *    This software is licensed under the Apache License, Version 2.0 (the
 *    "License") as published by the Apache Software Foundation.
 *
 *    You may not use this file except in compliance with the License. You may
 *    obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *    License for the specific language governing permissions and limitations
 *    under the License.
 *
 */

package io.supertokens.storage.couchbase;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.*;
import io.supertokens.pluginInterface.exceptions.DbInitException;
import io.supertokens.storage.couchbase.config.Config;
import io.supertokens.storage.couchbase.config.CouchbaseConfig;
import io.supertokens.storage.couchbase.output.Logging;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.Duration;
import java.util.Objects;

public class ConnectionPool extends ResourceDistributor.SingletonResource {

    private static final String RESOURCE_KEY = "io.supertokens.storage.couchbase.ConnectionPool";
    private Cluster cluster;
    private Bucket bucket;

    private final Start start;

    private ConnectionPool(Start start) {
        this.start = start;
    }

    private synchronized void initialiseCouchbaseClient() {
        if (this.cluster != null) {
            return;
        }

        if (!start.enabled) {
            throw new CouchbaseException("Connection refused");
        }

        CouchbaseConfig userConfig = Config.getConfig(start);

//        String connectionURI;
//        if (userConfig.useConnectionURIAsIs()) {
//            connectionURI = userConfig.getConnectionURI();
//        } else {

        String scheme = userConfig.getConnectionScheme();

        String hostName = userConfig.getHostName();

//        String port = userConfig.getPort() + "";
//        if (!port.equals("-1")) {
//            port = ":" + port;
//        } else {
//            port = "";
//        }

//        String attributes = userConfig.getConnectionAttributes();
//        if (!attributes.isEmpty()) {
//            attributes = "?" + attributes;
//        }

        String user = userConfig.getUser();
        String password = userConfig.getPassword();
//        String userInfo = "";
//        if (user != null) {
//            userInfo = user;
//        }
//        if (password != null) {
//            userInfo += ":" + password;
//        }
//        if (!userInfo.isEmpty()) {
//            userInfo += "@";
//        }

        // We omit database on purpose since that causes auth issues. Database is selected when
        // we fetch a connection.
//        connectionURI = scheme + "://" + userInfo + hostName + port + "/" + attributes;

        cluster = Cluster.connect(
                scheme + "://" + hostName,
                ClusterOptions.clusterOptions(user, password).environment(env -> {
                    // Customize client settings by calling methods on the "env" variable.

                })
        );

//        }

//        cluster = MongoClients
//                .create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(connectionURI))
//                        .applyToClusterSettings(builder -> builder.serverSelectionTimeout(5000, TimeUnit.MILLISECONDS))
//                        .build());

        // we have this below because there is a chance where this server is started before couchbase. So we must wait
        // for that to start, else this service will crash.

        // The below does not check for password or user being correct. But that is OK since then subsequent queries
        // will simply fail
        try {
            bucket = cluster.bucket(userConfig.getBucketName());
            bucket.waitUntilReady(Duration.ofSeconds(10));
//            ClientSession session = cluster.startSession();
//            session.close();
            // this means we have connected successfully
        } catch (CouchbaseException e) {
            if (!e.getMessage().contains("bucket doesn't exist")) {
                throw e;
            }
            e.printStackTrace();
            // this means we have connected successfully
        }

    }

    private static int getTimeToWaitToInit(Start start) {
        int actualValue = 3600 * 1000;
        if (Start.isTesting) {
            Integer testValue = ConnectionPoolTestContent.getInstance(start)
                    .getValue(ConnectionPoolTestContent.TIME_TO_WAIT_TO_INIT);
            return Objects.requireNonNullElse(testValue, actualValue);
        }
        return actualValue;
    }

    private static int getRetryIntervalIfInitFails(Start start) {
        int actualValue = 10 * 1000;
        if (Start.isTesting) {
            Integer testValue = ConnectionPoolTestContent.getInstance(start)
                    .getValue(ConnectionPoolTestContent.RETRY_INTERVAL_IF_INIT_FAILS);
            return Objects.requireNonNullElse(testValue, actualValue);
        }
        return actualValue;
    }

    private static ConnectionPool getInstance(Start start) {
        return (ConnectionPool) start.getResourceDistributor().getResource(RESOURCE_KEY);
    }

    private static void removeInstance(Start start) {
        start.getResourceDistributor().removeResource(RESOURCE_KEY);
    }

    static boolean isAlreadyInitialised(Start start) {
        return getInstance(start) != null && getInstance(start).cluster != null;
    }

    static void initPool(Start start) throws DbInitException {
        if (isAlreadyInitialised(start)) {
            return;
        }
        if (Thread.currentThread() != start.mainThread) {
            throw new DbInitException("Should not come here");
        }
        Logging.info(start, "Setting up Couchbase connection.", true);
        boolean longMessagePrinted = false;
        long maxTryTime = System.currentTimeMillis() + getTimeToWaitToInit(start);
        String errorMessage = "Error connecting to Couchbase instance. Please make sure that Couchbase is running and that "
                + "you have" + " specified the correct value for 'couchbase_connection_uri' in your " + "config file";
        try {
            ConnectionPool con = new ConnectionPool(start);
            start.getResourceDistributor().setResource(RESOURCE_KEY, con);
            while (true) {
                try {
                    con.initialiseCouchbaseClient();
                    break;
                } catch (Exception e) {
                    if (e.getMessage().contains("Connection refused") || (e instanceof com.couchbase.client.core.error.CouchbaseException
                            && e.getMessage().contains("Prematurely reached end of stream"))) {
                        start.handleKillSignalForWhenItHappens();
                        if (System.currentTimeMillis() > maxTryTime) {
                            throw new DbInitException(errorMessage);
                        }
                        if (!longMessagePrinted) {
                            longMessagePrinted = true;
                            Logging.info(start, errorMessage, true);
                        }
                        double minsRemaining = (maxTryTime - System.currentTimeMillis()) / (1000.0 * 60);
                        NumberFormat formatter = new DecimalFormat("#0.0");
                        Logging.info(start,
                                "Trying again in a few seconds for " + formatter.format(minsRemaining) + " mins...",
                                true);
                        try {
                            if (Thread.interrupted()) {
                                throw new InterruptedException();
                            }
                            Thread.sleep(getRetryIntervalIfInitFails(start));
                        } catch (InterruptedException ex) {
                            throw new DbInitException(errorMessage);
                        }
                    } else {
                        throw e;
                    }
                }
            }
        } finally {
            start.removeShutdownHook();
        }
    }

    public static Cluster getClientConnectedToCluster(Start start) {
        if (getInstance(start) == null) {
            throw new RuntimeException("Please call initPool before getConnection");
        }
        if (!start.enabled) {
            throw new CouchbaseException("Storage layer disabled");
        }
//        return getInstance(start).cluster.getDatabase(Config.getConfig(start).getBucketName());
        return getInstance(start).cluster;
    }

    public static Bucket getClientConnectedToBucket(Start start) {
        if (getInstance(start) == null) {
            throw new RuntimeException("Please call initPool before getConnection");
        }
        if (!start.enabled) {
            throw new CouchbaseException("Storage layer disabled");
        }
//        return getInstance(start).cluster.getDatabase(Config.getConfig(start).getBucketName());
        return getInstance(start).bucket;
    }

    static void close(Start start) {
        if (getInstance(start) == null) {
            return;
        }
        getInstance(start).cluster.close();
        removeInstance(start);
    }
}
