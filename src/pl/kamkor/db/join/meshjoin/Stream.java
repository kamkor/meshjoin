package pl.kamkor.db.join.meshjoin;



/** Tuples stream for MeshJoin
 * 
 * @author Kamil Korzekwa
 */
public interface Stream {
		
	/** 
	 * @param wStreamTuples
	 * @return next wStreamTuples from stream.
	 */
	Tuple[] read(int wStreamTuples);

}
