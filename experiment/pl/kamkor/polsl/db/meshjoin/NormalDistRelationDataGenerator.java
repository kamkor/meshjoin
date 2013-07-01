package pl.kamkor.polsl.db.meshjoin;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.log4j.Logger;

import pl.kamkor.db.DbUtil;

/** Generations Normal Distrubition records and inserts them into relation R. 
 * 
 * @author Kamil Korzekwa
 */
public class NormalDistRelationDataGenerator {
	
	private static final Logger LOGGER = Logger.getLogger(NormalDistRelationDataGenerator.class);	
	
	public static void main(String[] args) throws SQLException, URISyntaxException, IOException {
		final MeshJoinProperties properties = new MeshJoinProperties();
		Connection connection = properties.createConnection();
		if (connection == null) {
			LOGGER.error("No connection to database.");
			return;
		}
		
		connection.setAutoCommit(false);		
		PreparedStatement statement = null;
		Statement deleteStatement = null;
		try {			
			// delete old data
			deleteStatement = connection.createStatement();
			deleteStatement.executeUpdate("truncate table Synthetic_Data");
			
			// insert new data
			statement = connection.prepareStatement("insert into Synthetic_Data(join_key) values(?)");
			NormalDistribution normDist = new NormalDistribution(properties.getNormalDistMean(), properties.getNormalDistStdDev());				
			int i = 0;
			for (double sample : normDist.sample(properties.getRelationTuplesSize())) {				
				statement.setLong(1, Math.round(sample));
				statement.addBatch();
				if (i++ % 100000 == 0) {
					statement.executeBatch();
					LOGGER.info("executing batch: " + i);
				}				
			}	
			statement.executeBatch();
			connection.commit();
		} catch (SQLException e) {
			LOGGER.error(e.getMessage(), e);
			connection.rollback();						
		} finally {
			DbUtil.close(deleteStatement);
			DbUtil.close(statement);			
			connection.close();
		}
		
	}

}
