package pl.kamkor.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

/** 
 * Utility class for simple connection initialisation. For use in prototype/small projects.
 * 
 * @author Kamil Korzekwa
 */
public class Connections {
	
	private static final Logger LOGGER = Logger.getLogger(Connections.class);
	
	static {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			LOGGER.warn("PostgreSQL driver not found.");
		}
	}
	
	/** Creates new PostgreSQL connection.
	 *  
	 * @param hostname
	 * @param port
	 * @param dbName
	 * @param username
	 * @param password
	 * @return connection or null
	 */
	public static Connection createPostgreSQLConnection(
			String hostname, String port, String dbName, String username, String password) {
		Connection connection = null;
		try {
			String url = String.format("jdbc:postgresql://%s:%s/%s", hostname, port, dbName);
			connection = DriverManager.getConnection(url, username, password);
		} catch (SQLException e) {
			LOGGER.error(e.getMessage(), e);			
		}	
		return connection;
	}

}
