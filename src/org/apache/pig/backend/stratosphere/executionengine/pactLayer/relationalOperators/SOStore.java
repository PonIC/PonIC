package org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.pig.SortInfo;
import org.apache.pig.SStoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.PactOperator;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.Result;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.SOStatus;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans.PactPlanVisitor;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.Illustrator;

public class SOStore extends PactOperator {
	
	private static final long serialVersionUID = 1L;
    private static Result empty = new Result(SOStatus.STATUS_NULL, null);
    transient private SStoreFuncInterface storer;    
    transient private SOStoreImpl impl;
    private FileSpec sFile;
    private Schema schema;
    
    transient private Counter outputRecordCounter = null;

    // flag to distinguish user stores from MRCompiler stores.
    private boolean isTmpStore;
    
    // flag to distinguish single store from multiquery store.
    private boolean isMultiStore;
    
    // flag to indicate if the custom counter should be disabled.
    private boolean disableCounter = false;
    
    // the index of multiquery store to track counters
    private int index;
    
    // If we know how to reload the store, here's how. The lFile
    // FileSpec is set in PigServer.postProcess. It can be used to
    // reload this store, if the optimizer has the need.
    private FileSpec lFile;
    
    // if the predecessor of store is Sort (order by)
    // then sortInfo will have information of the sort 
    // column names and the asc/dsc info
    private SortInfo sortInfo;
    
    private String signature;

	public SOStore(OperatorKey k) {
		super(k);
	}

	@Override
	public void setIllustrator(Illustrator illustrator) {
		// do nothing
	}

	@Override
	public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
		// do nothing
		return null;
	}

	@Override
	public boolean supportsMultipleInputs() {
		return false;
	}

	@Override
	public boolean supportsMultipleOutputs() {
		return true;
	}

	@Override
	public String name() {
		 return (sFile != null) ? getAliasString() + "Store" + "("
	                + sFile.toString() + ")" + " - " + mKey.toString()
	                : getAliasString() + "Store" + "(" + "DummyFil:DummyLdr" + ")"
	                        + " - " + mKey.toString();
	}

	public void setSFile(FileSpec sFile) {
		this.sFile = sFile;		
	}
	
	public FileSpec getSFile() {
	    return sFile;
	}

	public void setInputSpec(FileSpec lFile) {
		this.lFile = lFile;
		
	}
	
    public FileSpec getInputSpec() {
        return lFile;
    }

	public void setSignature(String signature) {
		this.signature = signature;		
	}

	public void setSortInfo(SortInfo sortInfo) {
		this.sortInfo = sortInfo;		
	}
	
    public SortInfo getSortInfo() {
        return sortInfo;
    }
    
    public String getSignature() {
        return signature;
    }

	public void setIsTmpStore(boolean tmp) {
		 isTmpStore = tmp;
		
	}
	
    public boolean isTmpStore() {
        return isTmpStore;
    }

	public void setSchema(Schema schema) {
		this.schema = schema;
		
	}
	
	  public Schema getSchema() {
	        return schema;
	    }
	  
	    public SStoreFuncInterface getStoreFunc() {
	        if(storer == null){
	            storer = (SStoreFuncInterface)PigContext.instantiateFuncFromSpec(sFile.getFuncSpec());
	            storer.setStoreFuncUDFContextSignature(signature);
	        }
	        return storer;
	    }

	    public void setIndex(int index) {
	        this.index = index;
	    }

	    public int getIndex() {
	        return index;
	    }

	    public void setDisableCounter(boolean disableCounter) {
	        this.disableCounter = disableCounter;
	    }

	    public boolean disableCounter() {
	        return disableCounter;
	    }
	    
	    @Override
	    public void visit(PactPlanVisitor v) throws VisitorException {
	        v.visitStore(this);
	    }
	    
	    @Override
	    public Result getNext(Tuple t) throws ExecException {
	        Result res = processInput();
	        try {
	            switch (res.returnStatus) {
	            case SOStatus.STATUS_OK:
	                storer.putNext((Tuple)res.result);
	                res = empty;
	                break;
	            case SOStatus.STATUS_EOP:
	                break;
	            case SOStatus.STATUS_ERR:
	            case SOStatus.STATUS_NULL:
	            default:
	                break;
	            }
	        } catch (IOException ioe) {
	            int errCode = 2135;
	            String msg = "Received error from store function." + ioe.getMessage();
	            throw new ExecException(msg, errCode, ioe);
	        }
	        return res;
	    }
}
