package org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans;

import org.apache.pig.backend.stratosphere.executionengine.pactLayer.PactOperator;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.Add;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.Divide;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.EqualToExpr;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.GTOrEqualToExpr;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.GreaterThanExpr;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.LTOrEqualToExpr;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.LessThanExpr;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.Mod;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.Multiply;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.NotEqualToExpr;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.POAnd;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.POBinCond;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.POCast;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.POIsNull;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.POMapLookUp;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.PONegative;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.PONot;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.POOr;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.POProject;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.POUserComparisonFunc;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.Subtract;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOCoGroup;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOCross;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOFilter;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOForEach;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOLoad;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOMatch;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOReduce;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.relationalOperators.SOStore;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

public class PactPlanVisitor extends PlanVisitor<PactOperator, PactPlan>{

	/**
	 * A visitor for navigating and operating on a plan of PactOperators. 
	 * This class contains the logic to traverse the plan. 
	 * The implementation of visiting individual nodes is left to the PlanCompiler
	 * @author Vasiliki Kalavri
	 *
	 */
    protected PactPlanVisitor(PactPlan plan,
			PlanWalker<PactOperator, PactPlan> walker) {
		super(plan, walker);
	}

	public void visitLoad(SOLoad ld) throws VisitorException{
        //do nothing
    }

	public void visitFilter(SOFilter soFilter) throws VisitorException {
		// TODO Auto-generated method stub
		
	}

	public void visitStore(SOStore soStore) throws VisitorException {
		// TODO Auto-generated method stub
		
	}

	public void visitAdd(Add add) {
		// do nothing
		
	}

	public void visitConstant(ConstantExpression constantExpression) {
		// do nothing
		
	}

	public void visitDivide(Divide divide) {
		// do nothing
		
	}

	public void visitEqualTo(EqualToExpr equalToExpr) {
		// do nothing
		
	}

	public void visitGreaterThan(GreaterThanExpr greaterThanExpr) {
		// do nothing
		
	}

	public void visitGTOrEqual(GTOrEqualToExpr gtOrEqualToExpr) {
		// do nothing
		
	}

	public void visitLessThan(LessThanExpr lessThanExpr) {
		// do nothing
		
	}

	public void visitLTOrEqual(LTOrEqualToExpr ltOrEqualToExpr) {
		// do nothing
		
	}

	public void visitMod(Mod mod) {
		// do nothing
		
	}

	public void visitMultiply(Multiply multiply) {
		// do nothing
		
	}

	public void visitNotEqualTo(NotEqualToExpr notEqualToExpr) {
		// do nothing
		
	}

	public void visitAnd(POAnd poAnd) {
		// do nothing
		
	}

	public void visitBinCond(POBinCond poBinCond) {
		// do nothing
		
	}

	public void visitCast(POCast poCast) {
		// do nothing
		
	}

	public void visitIsNull(POIsNull poIsNull) {
		// do nothing
		
	}

	public void visitMapLookUp(POMapLookUp poMapLookUp) {
		// do nothing
		
	}

	public void visitNegative(PONegative poNegative) {
		// do nothing
		
	}

	public void visitNot(PONot poNot) {
		// do nothing
		
	}

	public void visitOr(POOr poOr) {
		// do nothing
		
	}

	public void visitProject(POProject poProject) {
		// do nothing
		
	}

	public void visitComparisonFunc(POUserComparisonFunc poUserComparisonFunc) {
		// do nothing
		
	}

	public void visitUserFunc(POUserFunc poUserFunc) {
		// do nothing
		
	}

	public void visitSubtract(Subtract subtract) {
		// do nothing
		
	}

	public void visitCross(SOCross soCross) {
		// do nothing
		
	}

	public void visitMatch(SOMatch soMatch) {
		// do nothing
		
	}

	public void visitSOForEach(SOForEach soForEach) {
		// do nothing
		
	}

	public void visitReduce(SOReduce soReduce) {
		// do nothing
		
	}

	public void visitCoGroup(SOCoGroup soCoGroup) {
		// do nothing
		
	}

}
