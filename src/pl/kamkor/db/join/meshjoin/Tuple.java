package pl.kamkor.db.join.meshjoin;

/**
 * Represents a single stream tuple or relation tuple.
 * 
 * @author Kamil Korzekwa
 */
public interface Tuple {
			
	/** 
	 * @return key by which tuples of Stream are joined with tuples of Relation
	 */
	JoinKey getJoinKey();
	
	/** 
	 * @return time when tuple arrived in memory.
	 */
	long getArrivalTime();	

}
