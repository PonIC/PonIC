package org.apache.pig.backend.stratosphere.executionengine.pactLayer;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigLogger;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigProgressable;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans.PactPlan;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans.PactPlanVisitor;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.Illustrable;
import org.apache.pig.pen.Illustrator;
import org.apache.pig.pen.util.LineageTracer;

public abstract class PactOperator extends Operator<PactPlanVisitor> implements Illustrable, Cloneable, Serializable {

	// The degree of parallelism requested
    protected int requestedParallelism;

    // The inputs that this operator will read data from
    protected List<PactOperator> inputs;

    // The outputs that this operator will write data to
    // Will be used to create Targeted tuples
    protected List<PactOperator> outputs;

    // The data type for the results of this operator
    protected byte resultType = DataType.TUPLE;

    // The physical plan this operator is part of
    protected PactPlan parentPlan;

    // Specifies if the input has been directly attached
    protected boolean inputAttached = false;

    // If inputAttached is true, input is set to the input tuple
    protected Tuple input = null;

    // The result of performing the operation along with the output
    protected Result res = null;


    // alias associated with this PactOperator
    protected String alias = null;
    
    // the key columns (if any)
    protected int keyPosition1;
    protected int keyPosition2;
    
    public Set<String> UDFs;
    
    public Set<PactOperator> scalars;
    
    // Will be used by operators to report status or transmit heartbeat
    // Should be set by the backends to appropriate implementations that
    // wrap their own version of a reporter.
    public static PigProgressable reporter;

    // Will be used by operators to aggregate warning messages
    // Should be set by the backends to appropriate implementations that
    // wrap their own version of a logger.
    protected static PigLogger pigLogger;

    // Dummy types used to access the getNext of appropriate
    // type. These will be null
    static final protected DataByteArray dummyDBA = null;

    static final protected String dummyString = null;

    static final protected Double dummyDouble = null;

    static final protected Float dummyFloat = null;

    static final protected Integer dummyInt = null;

    static final protected Long dummyLong = null;

    static final protected Boolean dummyBool = null;

    static final protected Tuple dummyTuple = null;

    static final protected DataBag dummyBag = null;

    static final protected Map dummyMap = null;

    // TODO: This is not needed. But a lot of tests check serialized physical plans
    // that are sensitive to the serialized image of the contained physical operators.
    // So for now, just keep it. Later it'll be cleansed along with those test golden
    // files
    protected LineageTracer lineageTracer;

    protected transient Illustrator illustrator = null;

    private boolean accum;
    private transient boolean accumStart;
	public PactOperator(OperatorKey k) {
		super(k);
		UDFs = new HashSet<String>();
        scalars = new HashSet<PactOperator>();
	}
	
    public PactOperator(OperatorKey k, int rp, List<PactOperator> inp) {
        super(k);
        requestedParallelism = rp;
        inputs = inp;
        res = new Result();
        UDFs = new HashSet<String>();
        scalars = new HashSet<PactOperator>();
    }

	public PactOperator(OperatorKey k, int rp) {
		super(k);
        requestedParallelism = rp;
        UDFs = new HashSet<String>();
        scalars = new HashSet<PactOperator>();
	}

	protected static final long serialVersionUID = 1L;

	public void visit(PactPlanVisitor v) throws VisitorException {
		// TODO Auto-generated method stub
		
	}

	public Result getNext(Tuple t) throws ExecException {
		// TODO Auto-generated method stub
		return null;
	}
	
	public int getRequestedParallelism() {
        return requestedParallelism;
    }

    public void setRequestedParallelism(int requestedParallelism) {
        this.requestedParallelism = requestedParallelism;
    }

    public byte getResultType() {
        return resultType;
    }

    public String getAlias() {
        return alias;
    }

    protected String getAliasString() {
        return (alias == null) ? "" : (alias + ": ");
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public void setAccumulative() {
        accum = true;
    }

    public boolean isAccumulative() {
       return accum;
    }

    public void setAccumStart() {
       if (!accum) {
               throw new IllegalStateException("Accumulative is not turned on.");
       }
       accumStart = true;
    }

    public boolean isAccumStarted() {
    	return accumStart;
    }

    public void setAccumEnd() {
       if (!accum){
    	   throw new IllegalStateException("Accumulative is not turned on.");
       }
       accumStart = false;
    }

    public void setResultType(byte resultType) {
        this.resultType = resultType;
    }

    public List<PactOperator> getInputs() {
        return inputs;
    }

    public void setInputs(List<PactOperator> inputs) {
        this.inputs = inputs;
    }

    public boolean isInputAttached() {
        return inputAttached;
    }
    
    /**
     * A generic method for parsing input that either returns the attached input
     * if it exists or fetches it from its predecessor. If special processing is
     * required, this method should be overridden.
     *
     * @return The Result object that results from processing the input
     * @throws ExecException
     */
    public Result processInput() throws ExecException {

        Result res = new Result();
        if (input == null && (inputs == null || inputs.size()==0)) {
//            log.warn("No inputs found. Signaling End of Processing.");
            res.returnStatus = SOStatus.STATUS_EOP;
            return res;
        }

        if (!isInputAttached()) {
        	res.returnStatus = SOStatus.STATUS_EOP;
        	return res;	//AVK
            //return inputs.get(0).getNext(dummyTuple);	//DVK
        } else {
            res.result = input;
            res.returnStatus = (res.result == null ? SOStatus.STATUS_NULL: SOStatus.STATUS_OK);
            detachInput();
            return res;
        }
    }

    /**
     * Detaches any tuples that are attached
     *
     */
    public void detachInput() {
        input = null;
        this.inputAttached = false;
    }

    /**
     * Shorts the input path of this operator by providing the input tuple
     * directly
     *
     * @param t -
     *            The tuple that should be used as input
     */
    public void attachInput(Tuple t) {
        input = t;
        this.inputAttached = true;
    }

    public Result getNext(Object obj, byte dataType) throws ExecException {
        switch (dataType) {
        case DataType.BAG:
            return getNext((DataBag) obj);
        case DataType.BOOLEAN:
            return getNext((Boolean) obj);
        case DataType.BYTEARRAY:
            return getNext((DataByteArray) obj);
        case DataType.CHARARRAY:
            return getNext((String) obj);
        case DataType.DOUBLE:
            return getNext((Double) obj);
        case DataType.FLOAT:
            return getNext((Float) obj);
        case DataType.INTEGER:
            return getNext((Integer) obj);
        case DataType.LONG:
            return getNext((Long) obj);
        case DataType.MAP:
            return getNext((Map) obj);
        case DataType.TUPLE:
            return getNext((Tuple) obj);
        default:
            throw new ExecException("Unsupported type for getNext: " + DataType.findTypeName(dataType));
        }
    }
        
        public Result getNext(Integer i) throws ExecException {
            return res;
        }

        public Result getNext(Long l) throws ExecException {
            return res;
        }

        public Result getNext(Double d) throws ExecException {
            return res;
        }

        public Result getNext(Float f) throws ExecException {
            return res;
        }

        public Result getNext(String s) throws ExecException {
            return res;
        }

        public Result getNext(DataByteArray ba) throws ExecException {
            return res;
        }

        public Result getNext(Map m) throws ExecException {
            return res;
        }

        public Result getNext(Boolean b) throws ExecException {
            return res;
        }

        public Result getNext(DataBag db) throws ExecException {
            Result ret = null;
            DataBag tmpBag = BagFactory.getInstance().newDefaultBag();
            for(ret = getNext(dummyTuple);ret.returnStatus!=SOStatus.STATUS_EOP;ret=getNext(dummyTuple)){
                if(ret.returnStatus == SOStatus.STATUS_ERR) {
                    return ret;
                }
                tmpBag.add((Tuple)ret.result);
            }
            ret.result = tmpBag;
            ret.returnStatus = (tmpBag.size() == 0)? SOStatus.STATUS_EOP : SOStatus.STATUS_OK;
            return ret;
        }
        
        public static Object getDummy(byte dataType) throws ExecException {
            switch (dataType) {
            case DataType.BAG:
                return dummyBag;
            case DataType.BOOLEAN:
                return dummyBool;
            case DataType.BYTEARRAY:
                return dummyDBA;
            case DataType.CHARARRAY:
                return dummyString;
            case DataType.DOUBLE:
                return dummyDouble;
            case DataType.FLOAT:
                return dummyFloat;
            case DataType.INTEGER:
                return dummyFloat;
            case DataType.LONG:
                return dummyLong;
            case DataType.MAP:
                return dummyMap;
            case DataType.TUPLE:
                return dummyTuple;
            default:
                throw new ExecException("Unsupported type for getDummy: " + DataType.findTypeName(dataType));
            }
        }

        protected void cloneHelper(PactOperator op) {
            resultType = op.resultType;
        }

        /**
         * Reset internal state in an operator.  For use in nested pipelines
         * where operators like limit and sort may need to reset their state.
         * Limit needs it because it needs to know it's seeing a fresh set of
         * input.  Blocking operators like sort and distinct need it because they
         * may not have drained their previous input due to a limit and thus need
         * to be told to drop their old input and start over.
         */
        public void reset() {
        	
        }
        
    	public void setFirstKeyPosition(int col) {
    		this.keyPosition1 = col;
    	}
    	
    	public int getFirstKeyPosition() {
    		return this.keyPosition1;
    	}
    	
    	public void setSecondKeyPosition(int col) {
    		this.keyPosition2 = col;
    	}
    	
    	public int getSecondKeyPosition() {
    		return this.keyPosition2;
    	}

}

