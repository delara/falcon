package com.edgestream.worker.metrics.db.pathstore;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Truncate;
import com.edgestream.worker.storage.PathstoreClient;
import pathstore.client.PathStoreClientAuthenticatedCluster;
import pathstore.client.PathStoreSession;
import pathstore.common.PathStoreProperties;
import pathstore.sessions.PathStoreSessionManager;
import pathstore.sessions.SessionToken;


public class CloudMetricsPathstoreDB {

    private final PathStoreSession session;

    public CloudMetricsPathstoreDB() {
        PathStoreProperties.PROPERTIES_LOCATION = "C:\\Users\\jason\\IdeaProjects\\pathstore.properties";
        PathstoreClient.initialiseConnection();
        session = PathStoreClientAuthenticatedCluster.getInstance().pathStoreSession();
    }

    public void wipeOutMetricsTable(String table) {
        Truncate truncate = QueryBuilder.truncate("pathstore_edgestream_metrics", table);
        SessionToken sessionToken = PathStoreSessionManager.getInstance().getKeyspaceToken( "pathstore_edgestream_metrics-" + table + "-session");
        session.execute(truncate, sessionToken);
    }

    public PathStoreSession getDatabaseSession() {
        return this.session;
    }

}
