package pl.kamkor.polsl.db.meshjoin;

import pl.kamkor.db.join.meshjoin.JoinKey;
import pl.kamkor.db.join.meshjoin.Tuple;

public class TestTuple implements Tuple, JoinKey {
	
	private long arrivalTime = System.currentTimeMillis();	
	
	long id;
	long joinKey;
	long[] properties;		
	
	public TestTuple(int propertiesSize) {
		this.properties = new long[propertiesSize];
	}
	  	  
	@Override
	public JoinKey getJoinKey() {
		return this;
	}
	  
	@Override
	public long getArrivalTime() {
		return arrivalTime;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (joinKey ^ (joinKey >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TestTuple other = (TestTuple) obj;
		if (joinKey != other.joinKey)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TestTuple [arrivalTime=" + arrivalTime + ", id=" + id
				+ ", joinKey=" + joinKey + "]";
	}

}
