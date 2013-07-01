package pl.kamkor.db.join.meshjoin;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;




/** 
 * Algorithm MESHJOIN
 * Input: A relation R and a stream S.
 * Output: Stream R  S.
 * Parameters: w tuples of S and b pages of R.
 * Method:
 *   While (true)
 *     Read b pages from R and w tuples from S
 *     If queue Q is full Then
 *       Dequeue T from Q where T are w pointers
 *       Remove the tuples of hash H that correspond to T
 *     EndIf
 *     Add the w tuples of S in H
 *     Enqueue in Q, w pointers to the above tuples in H
 *     For each tuple r in the b pages of R
 *       Ouput r <> H
 *   EndWhile
 *   
 * @author Kamil Korzekwa
 *
 */
public class MeshJoin {
	
	private static final Logger LOGGER = Logger.getLogger(MeshJoin.class);
	
	/** Used to access Relation R tuples */
	private final RelationDAO dao;
	
	/** pages of relation R to read in each iteration*/
	private final int bRelationPages; 
	
	/** Used to access stream tuples */
	private final Stream stream;
	
	/** tuples of stream S to read in each iteration */
	private final int wStreamTuples;
	
	/** Each entry contains w tuples of H. 
	 *  Records the arrival order of the batches. */
	private final ArrayBlockingQueue<Tuple[]> queue;
	
	/** HashTable for in-memory S-tuples based on their join-key */
	private final Map<JoinKey, Tuple> hashTable = new HashMap<JoinKey, Tuple>();
	
	private final Output output;	
	
	public MeshJoin(RelationDAO dao, int bRelationPages, Stream stream, int wStreamTuples, int queueSize, Output output) {
		this.dao = dao;
		this.bRelationPages = bRelationPages;
		this.stream = stream;
		this.wStreamTuples = wStreamTuples;
		this.queue = new ArrayBlockingQueue<Tuple[]>(queueSize);		
		this.output = output;	
	}
	
	/**
	 * Runs 1 iteration of mesh join algorithm. It goes trough Relation R once. 
	 */
	public void iteration() {
		LOGGER.debug("Beginning MeshJoin iteration");
		// Get iterator for all sets of Relation b pages
		Iterator<Tuple[]> tuplesIterator = dao.read(bRelationPages).iterator();
		while (tuplesIterator.hasNext()) {
			Tuple[] relationTuples = tuplesIterator.next();
			readStream();
			
			for (Tuple relationTuple : relationTuples) {				
				Tuple streamTuple = hashTable.get(relationTuple.getJoinKey());
				if (streamTuple != null) {
					output.join(relationTuple, streamTuple);
				}
			}
		}			
		LOGGER.debug("End of MeshJoin iteration");
	}

	private void readStream() {		
		final Tuple[] streamTuples = stream.read(wStreamTuples);
				
		if (queue.remainingCapacity() < 1) {		
			// Dequeue T from Q where T are w pointers
			for (Tuple tuple : queue.poll()) {
				// Remove the tuples from hash H that correspond to T
				hashTable.remove(tuple.getJoinKey());
				output.removedFromHashTable(tuple);
			}
		}
		
		// Add the w tuples of S to H
		for (Tuple tuple : streamTuples) {
			hashTable.put(tuple.getJoinKey(), tuple);
		}
		
		// Enqueue in Q, w pointers to the above tuples to H
		if (!queue.offer(streamTuples)) {
			throw new IllegalStateException("Cannot add tuples to the queue.");
		}
	}
	
}
