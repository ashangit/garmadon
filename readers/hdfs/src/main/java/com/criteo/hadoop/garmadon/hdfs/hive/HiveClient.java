package com.criteo.hadoop.garmadon.hdfs.hive;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;

public class HiveClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveClient.class);

    private final String database;
    private final String jdbcUrl;
    private Connection connection;
    private Statement stmt;

    public HiveClient(String driverName, String jdbcUrl, String database, String location) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        this.database = database;
        this.jdbcUrl = jdbcUrl;
        // TODO discover mesos slave to contact
        connect();
        createDatabaseIfAbsent(location);
    }

    protected void connect() throws SQLException {
        connection = DriverManager.getConnection(jdbcUrl);
        stmt = connection.createStatement();
    }

    protected boolean execute(String query) throws SQLException {
        boolean res = false;

        int maxAttempts = 5;
        for (int retry = 1; retry <= maxAttempts; ++retry) {
            try {
                res = stmt.execute(query);
            } catch (SQLException sqlException) {
                if (ExceptionUtils.indexOfThrowable(sqlException, SocketException.class) != -1) {
                    String exMsg = String.format("Retry connecting to hive (%d/%d)", retry, maxAttempts);
                    if (retry <= maxAttempts) {
                        LOGGER.warn(exMsg, sqlException);
                        try {
                            Thread.sleep(1000 * retry);
                            connect();
                        } catch (Exception ignored) {
                        }
                    } else {
                        LOGGER.error(exMsg, sqlException);
                        throw sqlException;
                    }
                } else {
                    throw sqlException;
                }
            }
        }
        return res;
    }

    public void createDatabaseIfAbsent(String location) throws SQLException {
        String databaseCreation = "CREATE DATABASE IF NOT EXISTS " + database + " COMMENT 'Database for garmadon events' LOCATION '" + location + "'";
        LOGGER.info("Create database {} if not exists", database);
        LOGGER.debug("Execute hql: {}", databaseCreation);
        execute(databaseCreation);
    }

    protected void createTableIfNotExist(String table, MessageType schema, String location) throws SQLException {
        String hiveSchema = schema.getFields().stream().map(field -> {
            try {
                return field.getName() + " " + inferHiveType(field);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.joining(", "));

        String tableCreation = "CREATE EXTERNAL TABLE IF NOT EXISTS "
            + database + "." + table
            + "(" + hiveSchema + ")"
            + " PARTITIONED BY (day string)"
            + " STORED AS PARQUET"
            + " LOCATION '" + location + "'";
        LOGGER.info("Create table {} if not exists", table);
        LOGGER.debug("Execute hql: {}", tableCreation);
        execute(tableCreation);
    }

    public void createPartitionIfNotExist(String table, MessageType schema, String partition, String location) throws SQLException {
        createTableIfNotExist(table, schema, location);

        String partitionCreation = "ALTER TABLE "
            + database + "." + table
            + " ADD IF NOT EXISTS PARTITION (day='" + partition + "')"
            + " LOCATION '" + location + "/day=" + partition + "'";
        LOGGER.info("Create partition {} on {} if not exists", table, table);
        LOGGER.debug("Execute hql: {}", partitionCreation);
        execute(partitionCreation);
    }

    protected String inferHiveType(Type field) throws Exception {
        String fieldHiveType;
        if (field.asPrimitiveType().getPrimitiveTypeName().name().equals("BINARY")) {
            fieldHiveType = "string";
        } else if (field.asPrimitiveType().getPrimitiveTypeName().name().equals("INT32")) {
            fieldHiveType = "int";
        } else if (field.asPrimitiveType().getPrimitiveTypeName().name().equals("INT64")) {
            fieldHiveType = "bigint";
        } else if (field.asPrimitiveType().getPrimitiveTypeName().name().equals("FLOAT")) {
            fieldHiveType = "float";
        } else if (field.asPrimitiveType().getPrimitiveTypeName().name().equals("DOUBLE")) {
            fieldHiveType = "double";
        } else if (field.asPrimitiveType().getPrimitiveTypeName().name().equals("BOOLEAN")) {
            fieldHiveType = "boolean";
        } else {
            throw new Exception("Unsupported Data Type: " + field.asPrimitiveType().getPrimitiveTypeName().name());
        }

        if (field.isRepetition(Type.Repetition.REPEATED)) {
            fieldHiveType = "array<" + fieldHiveType + ">";
        }

        return fieldHiveType;
    }

    public void close() throws SQLException {
        if (stmt != null) stmt.close();
        if (connection != null) connection.close();
    }

    protected Statement getStmt() {
        return stmt;
    }

    public static void main(String[] args) throws SQLException {
        //new HiveClient("org.apache.hive.jdbc.HiveDriver",
        // "jdbc:hive2://mesos-slave066-pa4.central.criteo.preprod:31338/default;principal=hive/hadoop-hive-
        // metastore.marathon-pa4.central.criteo.preprod@PA4.HPC.CRITEO.PREPROD",
        // "garmadon", "/tmp/nfraison/garmadon");
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);

        new HiveClient("com.criteo.hadoop.garmadon.hdfs.hive.HiveDriverConsul",
            "hive-hiveserver2/default;principal=hive/hadoop-hive-metastore.marathon-pa4.central.criteo.preprod@PA4.HPC.CRITEO.PREPROD",
            "garmadon", "/tmp/nfraison/garmadon");
    }

}
