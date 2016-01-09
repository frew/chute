package io.rodeo.chute.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

// TODO: Config
public class ConnectionManager {
	public Connection createConnection() throws SQLException {
		return DriverManager.getConnection(
				"jdbc:mysql://localhost/chute_test"
				// + "?profileSQL=true"
				, "root", "test");
	}
	
	public void returnConnection(Connection conn) throws SQLException {
		conn.close();
	}
}
