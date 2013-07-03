package pl.kamkor.db.join.meshjoin;

/**
 * Output of MeshJoin
 * 
 * @author Kamil Korzekwa
 */
public interface Output {
	
	void join(Tuple t1, Tuple t2);

	void removedFromHashTable(Tuple tuple);

}
