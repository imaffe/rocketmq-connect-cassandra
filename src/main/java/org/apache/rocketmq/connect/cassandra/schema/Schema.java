package org.apache.rocketmq.connect.cassandra.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO refactor this class
public class Schema {
    private static final Logger LOGGER = LoggerFactory.getLogger(Schema.class);

    private static final String SQL = "select schema_name from information_schema.schemata";

    private static final List<String> IGNORED_DATABASES = new ArrayList<>(
        Arrays.asList(new String[] {"information_schema", "mysql", "performance_schema", "sys"})
    );

    private Connection connection;

    private Map<String, Database> dbMap;

    public Map<String, Set<String>> dbTableMap;

    public Map<String, Map<String, String>> tableFilterMap;

    public Schema(Connection connection) {
        this.connection = connection;
        this.dbTableMap = new HashMap<>();
        this.tableFilterMap = new HashMap<>();
    }

    public void load() throws SQLException {

        dbMap = new HashMap<>();

        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = connection.prepareStatement(SQL);
            rs = ps.executeQuery();

            while (rs.next()) {
                String dbName = rs.getString(1);
                if (!IGNORED_DATABASES.contains(dbName) && dbTableMap.keySet().contains(dbName)) {
                    Database database = new Database(dbName, connection, dbTableMap.get(dbName), tableFilterMap);
                    dbMap.put(dbName, database);
                }
            }

        } finally {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
        }

        for (Database db : dbMap.values()) {
            db.init();
        }

    }

    public Table getTable(String dbName, String tableName) {

        if (dbMap == null) {
            reload();
        }

        Database database = dbMap.get(dbName);
        if (database == null) {
            return null;
        }

        Table table = database.getTable(tableName);
        if (table == null) {
            return null;
        }

        return table;
    }

    private void reload() {

        while (true) {
            try {
                load();
                break;
            } catch (Exception e) {
                LOGGER.error("Reload schema error.", e);
            }
        }
    }

    public void reset() {
        dbMap = null;
    }

    public Map<String, Database> getDbMap() {
        return dbMap;
    }
}

