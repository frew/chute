package io.rodeo.chute.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

public class MySqlBulkGenerator {
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/chute_test", "root", "test");
		PreparedStatement insertStmt = conn.prepareStatement("INSERT INTO testb(id, comment, user_id) VALUES(?, ?, ?)");
		Random r = new Random();
		for (int i = 0; i < 1000000; i++) {
			insertStmt.setInt(1, r.nextInt());
			insertStmt.setString(2, "Hello " + i);
			insertStmt.setInt(3, r.nextInt());
			insertStmt.execute();
		}
	}
}