package pl.kamkor.polsl.db.meshjoin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.log4j.Logger;

import pl.kamkor.db.DbUtil;
import pl.kamkor.db.join.meshjoin.RelationDAO;
import pl.kamkor.db.join.meshjoin.Tuple;

/** DAO for MeshJoin experiment. Reads relation R tuples.
 * 
 * @author Kamil Korzekwa
 */
public class PostgreSQLDAO implements RelationDAO {
	
	private static final Logger LOGGER = Logger.getLogger(PostgreSQLDAO.class);
	
	private final Connection connection;
	private MeshJoinProperties properties;
	
	public PostgreSQLDAO(Connection connection, MeshJoinProperties properties) {		
		this.connection = connection;
		this.properties = properties;		
	}

	@Override
	public Iterable<Tuple[]> read(final int bPages) {	
		return new Iterable<Tuple[]>() {
			@Override
			public Iterator<Tuple[]> iterator() {				
				return new TupleIterator(properties.getRelationTuplesSize() / bPages);
			}			
		};	
	}	
	
	private class TupleIterator implements Iterator<Tuple[]> {
		
		private int currentTuple;
		private int fetchSize;
										
		public TupleIterator(int fetchSize) {			
			this.fetchSize = fetchSize;
		}

		@Override
		public boolean hasNext() {
			return currentTuple < properties.getRelationTuplesSize();			
		}

		@Override
		public Tuple[] next() {
			Tuple[] tuples = null;			
			if (currentTuple + fetchSize > properties.getRelationTuplesSize()) {				
				tuples = new Tuple[properties.getRelationTuplesSize() - currentTuple];
			} else {
				tuples = new Tuple[fetchSize];
			}
			
			PreparedStatement ps = null;			
			try {
				ps = connection.prepareStatement("select * From Synthetic_Data order by id asc limit ? offset ?");
				ps.setInt(1, tuples.length);
				ps.setInt(2, currentTuple);
				ResultSet rs = ps.executeQuery();
				int i = 0;				
				while (rs.next()) {
					TestTuple tuple = new TestTuple(10);
					int colIdx = 1;
					tuple.id = rs.getLong(colIdx++);
					tuple.joinKey = rs.getLong(colIdx++);
					for (int propertyIdx = 0; propertyIdx < tuple.properties.length; propertyIdx++) {
						tuple.properties[propertyIdx] = rs.getLong(colIdx++);
					}					
					tuples[i++] = tuple;
				}
			} catch (SQLException e) {
				LOGGER.error(e.getMessage(), e);
			} finally {
				currentTuple += tuples.length;
				DbUtil.close(ps);				
			}
			
			return tuples;			
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}

}
