package com.edgestream.worker.metrics.db.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;

import java.net.InetSocketAddress;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace;

public class testConnection {
    public static void main(String[] args) {
        CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("10.70.2.12", 9042))
                .withLocalDatacenter("datacenter1")
                .build();
        CreateKeyspace createKs = createKeyspace("connection_test").ifNotExists().withSimpleStrategy(1);
        session.execute(createKs.build());
//        session.close();
    }
}
