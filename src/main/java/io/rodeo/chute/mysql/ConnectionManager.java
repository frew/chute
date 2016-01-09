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
