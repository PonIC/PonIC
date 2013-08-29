package org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PlanPrinter;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.PactOperator;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

public class PactPlan extends OperatorPlan<PactOperator> implements Cloneable, Serializable {

	private static final long serialVersionUID = 1L;
	
    public void attachInput(Tuple t){
        List<PactOperator> roots = getRoots();
        for (PactOperator operator : roots) {
            operator.attachInput(t);
		}
    }

	public void detachInput() {
        for(PactOperator op : getRoots())
            op.detachInput();
	    }
	
	 /**
     * Write a visual representation of the Pact Plan
     * into the given output stream
     * @param out : OutputStream to which the visual representation is written
     */
    public void explain(OutputStream out) {
        explain(out, true);
    }

    /**
     * Write a visual representation of the Physical Plan
     * into the given output stream
     * @param out : OutputStream to which the visual representation is written
     * @param verbose : Amount of information to print
     */
    public void explain(OutputStream out, boolean verbose){
        PlanPrinter<PactOperator, PactPlan> mpp = new PlanPrinter<PactOperator, PactPlan>(
                this);
        mpp.setVerbose(verbose);

        try {
            mpp.print(out);
        } catch (VisitorException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Write a visual representation of the Physical Plan
     * into the given printstream
     * @param ps : PrintStream to which the visual representation is written
     * @param format : Format to print in
     * @param verbose : Amount of information to print
     */
    public void explain(PrintStream ps, String format, boolean verbose) {
        ps.println("#-----------------------------------------------");
        ps.println("# Pact Plan:");
        ps.println("#-----------------------------------------------");

        if (format.equals("text")) {
            explain((OutputStream)ps, verbose);
            ps.println("");
        }
        /* No support for dot printing - AVK */
        /*
        else if (format.equals("dot")) {
            DotPOPrinter pp = new DotPOPrinter(this, ps);
            pp.setVerbose(verbose);
            pp.dump();
        }*/
        ps.println("");
  }
    
    @Override
    public void connect(PactOperator from, PactOperator to)
            throws PlanException {
        
        super.connect(from, to);
        to.setInputs(getPredecessors(to));
    }
    

}
