package pl.kamkor.polsl.db.meshjoin;

import java.util.ArrayList;
import java.util.List;

import pl.kamkor.db.join.meshjoin.Output;
import pl.kamkor.db.join.meshjoin.Tuple;

public class TestOutput implements Output {
	
	private List<Long> timeDiffs = new ArrayList<Long>();
	private int joinCount;
			
	@Override
	public void join(Tuple t1, Tuple t2) {		
		timeDiffs.add(Math.abs(t1.getArrivalTime() - t2.getArrivalTime()));
		joinCount++;
	}

	@Override
	public void removedFromHashTable(Tuple tuple) {
		
	}

	public int getJoinCount() {
		return joinCount;
	}	

}
