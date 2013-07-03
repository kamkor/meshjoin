package pl.kamkor.db;

import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

/**
 * 
 * @author Kamil Korzekwa
 */
public class DbUtil {
	
	private static final Logger LOGGER = Logger.getLogger(DbUtil.class);
	
	public static void close(Statement statement) {
		try {
			if (statement != null) {
				statement.close();
			}				
		} catch (SQLException exception) {
			LOGGER.error(exception.getMessage(), exception);
		}
	}

}
