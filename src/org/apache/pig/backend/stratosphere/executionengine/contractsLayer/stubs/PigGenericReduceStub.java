package org.apache.pig.backend.stratosphere.executionengine.contractsLayer.stubs;

import java.io.IOException;

import org.apache.pig.backend.stratosphere.executionengine.pactLayer.PactOperator;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.ObjectSerializer;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;

public abstract class PigGenericReduceStub extends ReduceStub {

	protected PactRecord outputRecord = new PactRecord();
	
	private static final Tuple DUMMYTUPLE = null;
    
    //the PactOperator corresponding to this Input Contract
	protected PactOperator pactOp;
	
	/** 
	 * Setup before the map method is called on the records. 
	 * Retrieves the PactOperator plan that needs to be implemented in the <code>map()</code>
	 * and sets the {@link pactOp} attribute
	 **/
	@Override
	public void open(Configuration parameters) throws Exception{
		try {
			this.pactOp = (PactOperator) ObjectSerializer.deserialize(parameters.getString("pactOp", null));	
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	protected void runPipeline() throws IOException, InterruptedException {
		//do nothing
	}

}
