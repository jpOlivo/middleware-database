package middleware.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public enum MySqlDBConnectionFactory  implements DBConnectionFactory {
	BICICLETAS_DB("jdbc:mysql://localhost:3306/bicicletas_ciudad", "root", ""), METEOROLOGIA_DB(
			"jdbc:mysql://localhost:3306/meteorologia_ciudad", "root", ""), VEHICULOS_DB("jdbc:mysql://localhost:3306/transito_ciudad", "root", ""),
					METRO_DB("jdbc:mysql://localhost:3306/subte_ciudad", "root", "");

	private Connection connection;

	private MySqlDBConnectionFactory(String urlDB, String user, String pass) {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			connection = DriverManager.getConnection(urlDB, user, pass);
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Connection getConnection() {
		return connection;
	}
}
