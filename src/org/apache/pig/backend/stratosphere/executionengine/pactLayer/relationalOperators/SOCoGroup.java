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

public class SOCoGroup extends PactOperator {
	
	private static final long serialVersionUID = 1L;
	
	protected DataBag[] inputBags;

    protected Tuple[] data;

    protected transient Iterator<Tuple>[] its;
    
    protected Tuple tupleOfLastBag;
    
	public SOCoGroup(OperatorKey k) {
		super(k);
	}

	public SOCoGroup(OperatorKey k, int rp) {
		this(k, rp, null);
	}
	
	public SOCoGroup(OperatorKey k, List<PactOperator> inputs) {
        this(k, -1, inputs);
    }

    public SOCoGroup(OperatorKey k, int rp, List<PactOperator> inputs) {
        super(k, rp, inputs);
    }

    @Override
    public void visit(PactPlanVisitor v) throws VisitorException {
        v.visitCoGroup(this);
    }

    @Override
    public String name() {
        return getAliasString() + "SOCoGroup" + "["
                + DataType.findTypeName(resultType) + "]" + " - "
                + mKey.toString();
    }
	@Override
	public void setIllustrator(Illustrator illustrator) {
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
	public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
		return null;
	}
	    
}
