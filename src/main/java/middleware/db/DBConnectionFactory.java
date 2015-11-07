package middleware.db;

import java.sql.Connection;

public interface DBConnectionFactory {
	Connection getConnection();
}
