package pl.kamkor.polsl.db.meshjoin;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

import org.apache.log4j.Logger;

import pl.kamkor.db.Connections;

/** Properties for experiment read from MeshJoin.properties.
 * 
 * @author Kamil Korzekwa
 */
public class MeshJoinProperties {
	
	private static final Logger LOGGER = Logger.getLogger(MeshJoinProperties.class);
	
	private int relationTuplesSize;
	private int normalDistMean;
	private int normalDistStdDev;
	private int bRelationPages;
	
	private String dbHostname;
	private String dbPort;
	private String dbSid;
	private String dbUser;
	private String dbPass;
	
	public MeshJoinProperties() {
		Properties properties = new Properties();
		FileInputStream fis = null;
		try {
			File propFile = new File(NormalDistRelationDataGenerator.class.getResource("MeshJoin.properties").toURI());			
			fis = new FileInputStream(propFile);
			properties.load(fis);
			
			this.relationTuplesSize = Integer.valueOf(properties.getProperty("relationTuplesSize"));
			this.normalDistMean = Integer.valueOf(properties.getProperty("normalDistMean"));
			this.normalDistStdDev = Integer.valueOf(properties.getProperty("normalDistStdDev"));
			this.bRelationPages = Integer.valueOf(properties.getProperty("bRelationPages"));
			
			this.dbHostname = properties.getProperty("dbHostname");
			this.dbPort = properties.getProperty("dbPort");
			this.dbSid = properties.getProperty("dbSid");
			this.dbUser = properties.getProperty("dbUser");
			this.dbPass = properties.getProperty("dbPass");
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);		
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					LOGGER.error(e.getMessage(), e);					
				}
			}
		}		
	}
	
	public Connection createConnection() {
		return Connections.createPostgreSQLConnection(dbHostname, dbPort, dbSid, dbUser, dbPass);
	}

	public static Logger getLogger() {
		return LOGGER;
	}

	public int getRelationTuplesSize() {
		return relationTuplesSize;
	}

	public int getNormalDistMean() {
		return normalDistMean;
	}

	public int getNormalDistStdDev() {
		return normalDistStdDev;
	}

	public int getBRelationPages() {
		return bRelationPages;
	}

	public String getDbHostname() {
		return dbHostname;
	}

	public String getDbPort() {
		return dbPort;
	}

	public String getDbSid() {
		return dbSid;
	}

	public String getDbUser() {
		return dbUser;
	}

	public String getDbPass() {
		return dbPass;
	}
	
}
