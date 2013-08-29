/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.stratosphere.executionengine.contractsLayer.plans;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.POCast;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.POUserComparisonFunc;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans.PactPlan;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans.PactPlanVisitor;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.PactOperator;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.PlanWalker;

public class UDFFinder extends PactPlanVisitor {
    List<String> UDFs;
    DepthFirstWalker<PactOperator, PactPlan> dfw;
    
    public UDFFinder(){
        this(null, null);
    }
    
    public UDFFinder(PactPlan plan, PlanWalker<PactOperator, PactPlan> walker) {
        super(plan, walker);
        UDFs = new ArrayList<String>();
        dfw = new DepthFirstWalker<PactOperator, PactPlan>(null);
    }

    public List<String> getUDFs() {
        return UDFs;
    }
    
    public void setPlan(PactPlan plan){
        mPlan = plan;
        dfw.setPlan(plan);
        mCurrentWalker = dfw;
        UDFs.clear();
    }
    
    /*private void addUDFsIn(PhysicalPlan ep) throws VisitorException{
        udfFinderForExpr.setPlan(ep);
        udfFinderForExpr.visit();
        UDFs.addAll(udfFinderForExpr.getUDFs());
    }

    @Override
    public void visitFilter(POFilter op) throws VisitorException {
        addUDFsIn(op.getPlan());
    }

    @Override
    public void visitGenerate(POGenerate op) throws VisitorException {
        List<PhysicalPlan> eps = op.getInputPlans();
        for (PhysicalPlan ep : eps) {
            addUDFsIn(ep);
        }
    }*/

    
    @Override
    public void visitUserFunc(POUserFunc userFunc) {
        UDFs.add(userFunc.getFuncSpec().toString());
    }

    @Override
    public void visitComparisonFunc(POUserComparisonFunc compFunc) {
        UDFs.add(compFunc.getFuncSpec().toString());
    }
    
    @Override
    public void visitCast(POCast op) {
        if (op.getFuncSpec()!=null)
        UDFs.add(op.getFuncSpec().toString());
    }
    
}
