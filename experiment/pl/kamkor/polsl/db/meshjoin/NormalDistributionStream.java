package pl.kamkor.polsl.db.meshjoin;

import java.util.Random;

import org.apache.commons.math3.distribution.NormalDistribution;

import pl.kamkor.db.join.meshjoin.Stream;
import pl.kamkor.db.join.meshjoin.Tuple;

/**  
 * @author Kamil Korzekwa
 */
public class NormalDistributionStream implements Stream {
			
	private NormalDistribution normDist;
	private Random random;

	public NormalDistributionStream(MeshJoinProperties properties) {		
		this.normDist = new NormalDistribution(properties.getNormalDistMean(), properties.getNormalDistStdDev());
		this.random = new Random();
	}
	
	@Override
	public Tuple[] read(int wTuples) {
		Tuple[] tuples = new Tuple[wTuples];
		int i = 0;
		for (double sample : normDist.sample(wTuples)) {			
			TestTuple tuple = new TestTuple(1);
			tuple.id = random.nextLong();
			tuple.joinKey = Math.round(sample);
			for (int propertyIdx = 0; propertyIdx < tuple.properties.length; propertyIdx++) {
				tuple.properties[propertyIdx] = random.nextLong();
			}						
			tuples[i++] = tuple;
		}
		return tuples;
	}	

}
