package org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators;

import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.PactOperator;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.SOStatus;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans.PactPlan;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans.PactPlanVisitor;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.Result; 
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.Illustrator;

public class SOFilter extends PactOperator {

    private static final long serialVersionUID = 1L;

    // The expression plan
    protected PactPlan plan;

    // The root comparison operator of the expression plan
//    ComparisonOperator comOp;
    protected PactOperator comOp;
    

    // The operand type for the comparison operator needed
    // to call the comparison operators getNext with the
    // appropriate type
    byte compOperandType;

    public SOFilter(OperatorKey k) {
        this(k, -1, null);
    }

    public SOFilter(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    public SOFilter(OperatorKey k, List<PactOperator> inputs) {
        this(k, -1, inputs);
    }

    public SOFilter(OperatorKey k, int rp, List<PactOperator> inputs) {
        super(k, rp, inputs);
    }

    /**
     * Attaches the processed input tuple to the expression plan and checks if
     * comparison operator returns a true. If so the tuple is not filtered and
     * let to pass through. Else, further input is processed till a tuple that
     * can be passed through is found or EOP is reached.
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
        Result res = null;
        Result inp = null;
        while (true) {
            inp = processInput();
                        
            if (inp.returnStatus == SOStatus.STATUS_EOP
                    || inp.returnStatus == SOStatus.STATUS_ERR)
                break;
            if (inp.returnStatus == SOStatus.STATUS_NULL) {
                continue;
            }

            plan.attachInput((Tuple) inp.result);

            res = comOp.getNext(dummyBool);
            plan.detachInput();
            if (res.returnStatus != SOStatus.STATUS_OK 
                    && res.returnStatus != SOStatus.STATUS_NULL) 
                return res;

            if (res.result != null) {
                illustratorMarkup(inp.result, inp.result, (Boolean) res.result ? 0 : 1);
                if ((Boolean) res.result)
                  return inp;
            }
        }
        return inp;
    }

    @Override
    public String name() {
        return getAliasString() + "Filter" + "["
                + DataType.findTypeName(resultType) + "]" + " - "
                + mKey.toString();
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
    public void visit(PactPlanVisitor v) throws VisitorException {
        v.visitFilter(this);
    }

    public void setPlan(PactPlan plan) {
        this.plan = plan;
        comOp = plan.getLeaves().get(0);
    }

    public PactPlan getPlan() {
        return plan;
    }
    
    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
      if (illustrator != null) {
          int index = 0;
          for (int i = 0; i < illustrator.getSubExpResults().size(); ++i) {
              if (!illustrator.getSubExpResults().get(i)[0])
                  index += (1 << i);
          }
          if (index < illustrator.getEquivalenceClasses().size())
              illustrator.getEquivalenceClasses().get(index).add((Tuple) in);
          if (eqClassIndex == 0) // add only qualified record
              illustrator.addData((Tuple) out);
      }
      return (Tuple) out;
    }

	@Override
	public void setIllustrator(Illustrator illustrator) {
		
	}

}
