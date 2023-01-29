package org.example.postgres;

import java.sql.*;

/**
 * this class is used to connect to the database and get random drugs and pharmacies
 */
public class PostgresService {


    private static final String DB_URL = "jdbc:postgresql://localhost:5432/postgres";

    private final Statement statement;

    /**
     * constructor of the class PostgresService
     *
     * @throws Exception if the connection to the database fails
     */
    public PostgresService() throws Exception {
        Connection conn = DriverManager.getConnection(DB_URL, "postgres", "");
        this.statement = conn.createStatement();
    }

    /**
     * this method is used to get a random drug from the database
     *
     * @return a ResultSet containing the drug
     * @throws SQLException if the query fails
     */
    public ResultSet getDrugs() throws SQLException {
        return this.statement.executeQuery("SELECT CIP,prix FROM drugs4projet ORDER BY RANDOM() LIMIT 1;");
    }

    /**
     * this method is used to get a random pharmacy from the database
     *
     * @return a ResultSet containing the pharmacy
     * @throws SQLException if the query fails
     */
    public ResultSet getPharmacies() throws SQLException {
        return this.statement.executeQuery("SELECT id FROM pharm4projet ORDER BY RANDOM() LIMIT 1;");
    }

    /**
     * this method is used to close the connection to the database
     *
     * @throws SQLException if the connection fails to close
     */
    public void close() throws SQLException {
        this.statement.getConnection().close();
    }


}
