package org.apache.pig.backend.stratosphere.executionengine.contractsLayer.stubs;

import java.io.IOException;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.PactOperator;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.Result;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.SOStatus;
import org.apache.pig.backend.stratosphere.executionengine.util.DataTypeUtils;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.ObjectSerializer;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * We need to extend AbstractStub so that the map method can accept Tuple as a parameter
 * @author hinata
 *
 */
public abstract class PigGenericMapStub extends MapStub
{

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
		
		while(true){
            Result res = this.pactOp.getNext(DUMMYTUPLE);
                        
            if(res.returnStatus==SOStatus.STATUS_OK){
            	this.outputRecord = DataTypeUtils.tupleToRecord((Tuple)res.result);
                continue;
            }
            
            if(res.returnStatus==SOStatus.STATUS_EOP) {
                return;
            }
            
            if(res.returnStatus==SOStatus.STATUS_NULL)
                continue;
            
            if(res.returnStatus==SOStatus.STATUS_ERR){
                // if there is an errmessage use it
                String errMsg;
                if(res.result != null) {
                    errMsg = "Received Error while " +
                    "processing the map plan: " + res.result;
                } else {
                    errMsg = "Received Error while " +
                    "processing the map plan.";
                }
                    
                int errCode = 2055;
                ExecException ee = new ExecException(errMsg, errCode, PigException.BUG);
                throw ee;
            }
        }

	}

}
