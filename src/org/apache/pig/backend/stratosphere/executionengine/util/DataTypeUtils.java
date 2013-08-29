package org.apache.pig.backend.stratosphere.executionengine.util;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactString;

public class DataTypeUtils {
	
    protected static TupleFactory tf = TupleFactory.getInstance();

	public static Tuple recordToTuple(PactRecord record) {
		
		Tuple tuple = tf.newTuple(record.getNumFields());
				
		for(int i=0; i<record.getNumFields(); i++){
			try {
					tuple.set(i, record.getField(i, PactString.class).getValue());	
			} catch (ExecException e) {
				e.printStackTrace();
			}
		}
		
		return tuple;
	}
	
	
	public static PactRecord tupleToRecord(Tuple tuple) {
		PactRecord rec = new PactRecord(tuple.size());

		for(int i=0; i<tuple.size(); i++){
			try {
				rec.setField(i, new PactString(tuple.get(i).toString()));
			} catch (ExecException e) {
				e.printStackTrace();
			}
		}
		
		return rec;
	}
}
