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

import org.apache.pig.backend.stratosphere.executionengine.pactLayer.PactOperator;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans.PactPlan;
import org.apache.pig.backend.stratosphere.executionengine.pactLayer.plans.PactPlanVisitor;
import org.apache.pig.impl.plan.DepthFirstWalker;

public class ScalarPactFinder extends PactPlanVisitor {

	List<PactOperator> scalars = new ArrayList<PactOperator>();
    
    public ScalarPactFinder(PactPlan plan) {
        super(plan, new DepthFirstWalker<PactOperator, PactPlan>(plan));
    }

    public List<PactOperator> getScalars() {
        return scalars;
    }

    @Override
    public void visitUserFunc(POUserFunc userFunc) {
        if(userFunc.getReferencedOperator() != null) {
            scalars.add(userFunc.getReferencedOperator());
        }
    }
}
