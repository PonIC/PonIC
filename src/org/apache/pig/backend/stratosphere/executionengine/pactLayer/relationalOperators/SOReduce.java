package org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators;

import java.util.List;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.PactOperator;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans.PactPlanVisitor;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.Illustrator;

public class SOReduce extends PactOperator {

	public SOReduce(OperatorKey k, int rp) {
		this(k, rp, null);
	}
	
	public SOReduce(OperatorKey k, List<PactOperator> inputs) {
        this(k, -1, inputs);
    }

    public SOReduce(OperatorKey k, int rp, List<PactOperator> inputs) {
        super(k, rp, inputs);
    }
    
	@Override
	public void setIllustrator(Illustrator illustrator) {
	
	}

	@Override
	public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
		return null;
	}

	@Override
	public boolean supportsMultipleInputs() {
		return false;
	}

	@Override
	public boolean supportsMultipleOutputs() {
		return false;
	}

	 @Override
	    public String name() {
	        return getAliasString() + "SOReduce["
	                + DataType.findTypeName(resultType) + "]" + " - "
	                + mKey.toString();
	    }

	@Override
    public void visit(PactPlanVisitor v) throws VisitorException {
        v.visitReduce(this);
    }

}
