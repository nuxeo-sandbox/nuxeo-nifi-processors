/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.nuxeo.labs.nifi.processors;

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class ITExecuteNuxeoQueryTest extends BaseTest {

    private TestRunner testRunner;

    @Before
    public void init() throws Exception {
        initDocuments();
        
        testRunner = TestRunners.newTestRunner(ExecuteNuxeoQuery.class);
        addController(testRunner);

        testRunner.setProperty(ExecuteNuxeoQuery.NUXEO_CLIENT_SERVICE, "localhost");
    }

    @Test
    public void testProcessor() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("nx-query", "SELECT * from Document");
        attributes.put("nx-page-size", "5");
        attributes.put("nx-page-index", "0");
        attributes.put("nx-max-results", "1000");
        attributes.put("nx-sort-by", "dc:title");
        attributes.put("nx-sort-order", "desc");

        testRunner.enqueue("", attributes);
        testRunner.run(1);
        testRunner.assertTransferCount(ExecuteNuxeoQuery.REL_NEXT_PAGE, 1);
        testRunner.assertTransferCount(ExecuteNuxeoQuery.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(ExecuteNuxeoQuery.REL_SUCCESS, 5);
    }

}
