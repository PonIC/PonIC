package org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.LoadFunc;
import org.apache.pig.SLoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.stratosphere.datastorage.ConfigurationUtil;
import org.apache.pig.backend.stratosphere.executionengine.contractsLayer.PigDataSource;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.PactOperator;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.Result;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.SOStatus;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.ReadToEndLoader;
import org.apache.pig.impl.io.SReadToEndLoader;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.Illustrator;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans.PactPlanVisitor;
import org.apache.pig.builtin.SPigStorage;

import eu.stratosphere.nephele.configuration.Configuration;

public class SOLoad extends PactOperator {

	 public SOLoad(OperatorKey k) {
		super(k);
	}
	    	    
	 public SOLoad(OperatorKey k, SLoadFunc lf, String inputFile){
		 
		 this(k);
		 this.loader = lf;
		 this.inputFile = inputFile;
	    }

	private static final Log log = LogFactory.getLog(SOLoad.class);

    // The user defined load function or a default load function
    private transient SLoadFunc loader;
    // The filespec on which the operator is based
    FileSpec lFile;
    // PigContext passed to us by the operator creator
    PigContext pc;
    
    String inputFile;
    
    //Indicates whether the loader setup is done or not
    boolean setUpDone = false;
    // Alias for the POLoad
    private String signature;
    
    private long limit=-1;
    
	private static final long serialVersionUID = 1L;
	
	//this is the DataSource of the Pact Plan being compiled
    //it is created by the SReadToEndLoader.init()
    protected PigDataSource pds = null;
	
	public FileSpec getLFile() {
	     return lFile;
	}
	
	public void setLFile(FileSpec fileSpec) {
		lFile = fileSpec;
		
	}
	
	public String getInputFile() {
	     return inputFile;
	}
	
	public void setInputFile(String input) {
		inputFile = input;
		
	}
	
	   /**
     * Set up the loader by 
     * 1) Instantiating the load func
     * 2) Opening an input stream to the specified file and
     * 3) Binding to the input stream at the specified offset.
     * @throws IOException
     */
    public void setUp() throws IOException{
    	SPigStorage wrapped = new SPigStorage();
        loader = new SReadToEndLoader(wrapped, 
        		ConfigurationUtil.toConfiguration(pc.getProperties()), inputFile, 0);
    }
    
    /**
     * At the end of processing, the inputstream is closed
     * using this method
     * @throws IOException
     */
    public void tearDown() throws IOException{
        setUpDone = false;
    }
    
	/**
     * The main method used by this operator's successor
     * to read tuples from the specified file using the
     * specified load function.
     * 
     * @return Whatever the loader returns
     *          A null from the loader is indicative
     *          of EOP and hence the tearDown of connection
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
        if(!setUpDone && lFile!=null){
            try {
                setUp();
            } catch (IOException ioe) {
                int errCode = 2081;
                String msg = "Unable to setup the load function.";
                throw new ExecException(msg, errCode, PigException.BUG, ioe);
            }
            setUpDone = true;
        }
        Result res = new Result();
        try {
            res.result = loader.getNext();
            if(res.result==null){
                res.returnStatus = SOStatus.STATUS_EOP;
                tearDown();
            }
            else
                res.returnStatus = SOStatus.STATUS_OK;

            if (res.returnStatus == SOStatus.STATUS_OK)
                res.result = illustratorMarkup(res, res.result, 0);
        } catch (IOException e) {
            log.error("Received error from loader function: " + e);
            return res;
        }
        return res;
    }


	@Override
	public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
		return null;
	}

	@Override
	public void visit(PactPlanVisitor v) throws VisitorException {
		v.visitLoad(this);
	}

	@Override
	public boolean supportsMultipleInputs() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMultipleOutputs() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
    public String name() {
        return (lFile != null) ? getAliasString() + "Load" + "(" + lFile.toString()
                + ")" + " - " + mKey.toString() : getAliasString() + "Load" + "("
                + "DummyFil:DummyLdr" + ")" + " - " + mKey.toString();
    }




	public void setPc(PigContext pc) {
		this.pc = pc;
		
	}

	public void setSignature(String signature) {
		this.signature = signature;
		
	}
	
	public String getSignature() {
		return this.signature;
		
	}

	public void setLimit(long limit) {
		this.limit = limit;
		
	}

	@Override
	public void setIllustrator(Illustrator illustrator) {
		// TODO Auto-generated method stub
		
	}
	
	public PigDataSource getDataSource(){
		return this.pds;
	}

}
