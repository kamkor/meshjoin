package pl.kamkor.db.join.meshjoin;



/**
 * MeshJoin Relation R DAO
 */
public interface RelationDAO {
	
	/** 
	 * @param bRelationPages
	 * @return iterator where each array of Tuple contains bRelationPages of tuples. 
	 */
	Iterable<Tuple[]> read(int bRelationPages);
	
}
