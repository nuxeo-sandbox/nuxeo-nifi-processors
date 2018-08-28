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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class ITNuxeoBlobOperationTest extends BaseTest {

    private TestRunner testRunner;

    @Before
    public void init() throws Exception {
        initDocuments();

        testRunner = TestRunners.newTestRunner(NuxeoBlobOperation.class);
        addController(testRunner);

        testRunner.setProperty(NuxeoBlobOperation.TARGET_PATH, "${nxpath}");
        testRunner.setProperty(NuxeoBlobOperation.OPERATION_ID, "Blob.Attach");
        testRunner.setProperty(NuxeoBlobOperation.NUXEO_CLIENT_SERVICE, "localhost");
        testRunner.setProperty("document", "${nx-path}");
        testRunner.setProperty("xpath", "files:files");

    }

    @Test
    public void testProcessor() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("nx-path", FOLDER_2_FILE);

        testRunner.enqueue("data", attributes);
        testRunner.run(1);
        testRunner.assertTransferCount(NuxeoBlobOperation.REL_ORIGINAL, 1);
        testRunner.assertTransferCount(NuxeoBlobOperation.REL_SUCCESS, 1);
        testRunner.assertTransferCount(NuxeoBlobOperation.REL_FAILURE, 0);

        for (MockFlowFile mff : testRunner.getFlowFilesForRelationship(NuxeoBlobOperation.REL_SUCCESS)) {
            System.out.println(new String(mff.toByteArray()));
        }
    }

}
