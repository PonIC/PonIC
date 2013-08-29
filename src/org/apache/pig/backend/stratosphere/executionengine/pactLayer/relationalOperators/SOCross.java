package org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators;

import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.PactOperator;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.Result;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.SOStatus;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans.PactPlanVisitor;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.Illustrator;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.pen.util.LineageTracer;

public class SOCross extends PactOperator {
	
	private static final long serialVersionUID = 1L;
	
	protected DataBag[] inputBags;

    protected Tuple[] data;

    protected transient Iterator<Tuple>[] its;
    
    protected Tuple tupleOfLastBag;
    
	public SOCross(OperatorKey k) {
		super(k);
		// TODO Auto-generated constructor stub
	}

	public SOCross(OperatorKey k, int rp) {
		this(k, rp, null);
	}
	
	public SOCross(OperatorKey k, List<PactOperator> inputs) {
        this(k, -1, inputs);
    }

    public SOCross(OperatorKey k, int rp, List<PactOperator> inputs) {
        super(k, rp, inputs);
    }

    @Override
    public void visit(PactPlanVisitor v) throws VisitorException {
        v.visitCross(this);
    }

    @Override
    public String name() {
        return getAliasString() + "SOCross" + "["
                + DataType.findTypeName(resultType) + "]" + " - "
                + mKey.toString();
    }
	@Override
	public void setIllustrator(Illustrator illustrator) {
	}

	@Override
	public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
		 if (illustrator != null) {
	            ExampleTuple tOut = new ExampleTuple((Tuple) out);
	            illustrator.addData(tOut);
	            illustrator.getEquivalenceClasses().get(eqClassIndex).add(
	                    (Tuple) out);
	            LineageTracer lineageTracer = illustrator.getLineage();
	            lineageTracer.insert(tOut);
	            for (int i = 0; i < data.length; i++) {
	                lineageTracer.union(tOut, data[i]);
	            }
	            return tOut;
	        } else {
	            return (Tuple) out;
	        }
	}

	@Override
	public boolean supportsMultipleInputs() {
		return true;
	}

	@Override
	public boolean supportsMultipleOutputs() {
		return false;
	}
	
	 @Override
	    public Result getNext(Tuple t) throws ExecException {
	        Result res = new Result();
	        int noItems = inputs.size();
	        if (inputBags == null) {
	            accumulateData();
	            if (!loadLastBag()) {
	                res.returnStatus = SOStatus.STATUS_EOP;
	                clearMemory();
	                return res;
	            }
	        }

	        if (its != null) {
	            // we check if we are done with processing
	            // we do that by checking if all the iterators are used up
	            boolean finished = true;
	            boolean empty = false;
	            for (int i = 0; i < its.length; i++) {
	                if (inputBags[i].size() == 0) {
	                    empty = true;
	                    break;
	                }
	                finished &= !its[i].hasNext();
	            }
	            if (empty) {
	                // if one bag is empty, there doesn't exist non-null cross product.
	                // simply clear all the input tuples of the first bag and finish.
	                int index = inputs.size() - 1;
	                for (Result resOfLastBag = inputs.get(index).getNext(dummyTuple); resOfLastBag.returnStatus !=
	                    SOStatus.STATUS_EOP; resOfLastBag = inputs.get(index).getNext(dummyTuple));
	                res.returnStatus = SOStatus.STATUS_EOP;
	                clearMemory();
	                return res;
	            } else if (finished && !loadLastBag()) {
	                res.returnStatus = SOStatus.STATUS_EOP;
	                clearMemory();
	                return res;
	            }

	        }

	        if (data == null) {
	            // getNext being called for the first time or starting on new input
	            // data we instantiate the template array and start populating it
	            // with data
	            data = new Tuple[noItems];
	            data[noItems - 1] = tupleOfLastBag;
	            for (int i = 0; i < noItems - 1; ++i) {
	                data[i] = its[i].next();

	            }
	            res.result = createTuple(data);
	            res.returnStatus = SOStatus.STATUS_OK;
	            return res;
	        } else {
	            data[noItems - 1] = tupleOfLastBag;
	            int length = noItems - 1;
	            for (int index = 0; index < length; ++index) {
	                if (its[index].hasNext()) {
	                    data[index] = its[index].next();
	                    res.result = createTuple(data);
	                    res.returnStatus = SOStatus.STATUS_OK;
	                    return res;
	                } else {
	                    // reset this index's iterator so cross product can be
	                    // achieved we would be resetting this way only for the
	                    // indexes from the end when the first index which needs to
	                    // be flattened has reached the last element in its
	                    // iterator, we won't come here - instead, we reset all
	                    // iterators at the beginning of this method.
	                    its[index] = (inputBags[index]).iterator();
	                    data[index] = its[index].next();
	                }
	            }
	            res.result = createTuple(data);
	            res.returnStatus = SOStatus.STATUS_OK;
	            return res;
	        }
	    }
	 
	 @SuppressWarnings("unchecked")
	    private void accumulateData() throws ExecException {
	        int count = 0;
	        int length = inputs.size() - 1;
	        inputBags = new DataBag[length];
	        its = new Iterator[length];
	        for (int i = 0; i < length; ++i) {
	            PactOperator op = inputs.get(i);
	            DataBag bag = BagFactory.getInstance().newDefaultBag();
	            inputBags[count] = bag;
	            for (Result res = op.getNext(dummyTuple); res.returnStatus != SOStatus.STATUS_EOP; res = op
	                    .getNext(dummyTuple)) {
	                if (res.returnStatus == SOStatus.STATUS_NULL)
	                    continue;
	                if (res.returnStatus == SOStatus.STATUS_ERR)
	                    throw new ExecException(
	                            "Error accumulating data in the local Cross operator");
	                if (res.returnStatus == SOStatus.STATUS_OK)
	                    bag.add((Tuple) res.result);
	            }
	            its[count++] = bag.iterator();
	        }
	    }

	    private Tuple createTuple(Tuple[] data) throws ExecException {
	        Tuple out = TupleFactory.getInstance().newTuple();

	        for (int i = 0; i < data.length; ++i) {
	            Tuple t = data[i];
	            int size = t.size();
	            for (int j = 0; j < size; ++j) {
	                out.append(t.get(j));
	            }
	        }

	        return illustratorMarkup(out, out, 0);
	    }
	    
	    private boolean loadLastBag() throws ExecException {
	        Result resOfLastBag = null;
	        int index = inputs.size() - 1;
	        for (resOfLastBag = inputs.get(index).getNext(dummyTuple); resOfLastBag.returnStatus ==
	                SOStatus.STATUS_NULL; inputs.get(index).getNext(dummyTuple));
	        switch (resOfLastBag.returnStatus) {
	        case SOStatus.STATUS_EOP:
	            return false;
	        case SOStatus.STATUS_OK:
	            // each time when an tuple of last bag is ejected, traverse all the
	            // combinations of the tuples from the other n - 1 bags to save the
	            // memory for one bag.
	            tupleOfLastBag = (Tuple) resOfLastBag.result;
	            return true;
	        case SOStatus.STATUS_ERR:
	        default:
	            throw new ExecException(
	                    "Error accumulating data in the local Cross operator");
	        }
	    }
	    
	    private void clearMemory() {
	        // reset inputBags, its, data and tupleOfLastBag to null so that in the
	        // next round of getNext, the new input data will be loaded.
	        tupleOfLastBag = null;
	        inputBags = null;
	        its = null;
	        data = null;
	    }


}
