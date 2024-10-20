package com.edgestream.worker.metrics.testing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DerbyTest {

    public static void main(String[] args) throws SQLException {


        String dbURL = "jdbc:derby:C:\\Users\\Brian\\IdeaProjects\\edgestream\\Worker\\metricsDB";


        Connection conn = DriverManager.getConnection(dbURL);

        System.out.println(conn.getClientInfo());
    }
}
