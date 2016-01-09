package io.rodeo.chute.mysql;

/*
Copyright 2016 Fred Wulff

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

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