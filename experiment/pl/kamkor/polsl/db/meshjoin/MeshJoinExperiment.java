package pl.kamkor.polsl.db.meshjoin;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import pl.kamkor.db.join.meshjoin.MeshJoin;
import pl.kamkor.db.join.meshjoin.Stream;

public class MeshJoinExperiment {
	
	private static final Logger LOGGER = Logger.getLogger(MeshJoinExperiment.class);
	
	private class Result {
		private long iterationTime;
		private long serviceRate;
		
		@Override
		public String toString() {
			return "Result [iterationTime=" + iterationTime + ", serviceRate="
					+ serviceRate + "]";
		}
	}

	private final Connection connection;
	private MeshJoinProperties properties;	
		
	public MeshJoinExperiment(Connection connection, MeshJoinProperties properties) {
		this.connection = connection;		
		this.properties = properties;		
	}

	public Result iteration() {
		final long before = System.currentTimeMillis();
		
		final TestOutput output = new TestOutput();
		final Stream stream = new NormalDistributionStream(properties);
		int w = 2000;
		int maxWTuples = (int) Math.round(w * 18.0 / (w * 60.0 / 1024 / 1024));
		int queueSize = maxWTuples / w; 
		final MeshJoin meshJoin = 
				new MeshJoin(
						new PostgreSQLDAO(connection, properties), 
						properties.getBRelationPages(), 
						stream, 
						w, 
						queueSize, 
						output);
		
		meshJoin.iteration();
		
		Result result = new Result();
		result.iterationTime = System.currentTimeMillis() - before;
		result.serviceRate = output.getJoinCount() / (result.iterationTime / 1000);
		LOGGER.info(result);
		return result; 
	}	
	
	public static void main(String[] args) throws SQLException {
		final MeshJoinProperties properties = new MeshJoinProperties();
		final Connection connection = properties.createConnection();
		if (connection == null) {
			LOGGER.error("No connection to database.");
			return;
		}
		
		MeshJoinExperiment test = new MeshJoinExperiment(connection, new MeshJoinProperties());
		Result result = test.iteration();
		LOGGER.info(result);
		connection.close();
	}

}
