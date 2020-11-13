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

import org.apache.commons.jxpath.JXPathContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ITNuxeoDocumentToAttributesTest extends BaseTest {

    private TestRunner testRunner;

    @Before
    public void init() throws Exception {
        initDocuments();

        testRunner = TestRunners.newTestRunner(NuxeoDocumentToAttributes.class);
        addController(testRunner);

        testRunner.setProperty(NuxeoDocumentToAttributes.DOC_PATH, "${nx-path}");
        testRunner.setProperty(NuxeoDocumentToAttributes.NUXEO_CLIENT_SERVICE, "localhost");
    }

    @Test
    public void testProcessor() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("nx-path", FOLDER_2_FILE);

        testRunner.enqueue("", attributes);
        testRunner.run(1);
        testRunner.assertTransferCount(NuxeoDocumentToAttributes.REL_FAILURE, 0);
        testRunner.assertTransferCount(NuxeoDocumentToAttributes.REL_SUCCESS, 1);

        testRunner.getFlowFilesForRelationship(NuxeoDocumentToAttributes.REL_SUCCESS).stream().forEach(
                doc -> doc.assertAttributeExists("dc:title"));
    }

    @Test
    public void testArrayPathQuery() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("test", 2.7f);
        Object[] vals = new Object[] { "a", 1, true, map };
        JXPathContext jx = JXPathContext.newContext(vals);
        jx.getVariables().declareVariable("root", vals);
        Assert.assertEquals(jx.getValue("$root[4]/test"), 2.7f);

    }

    @Test
    public void testComplexProperty() {
        testRunner.setProperty("fileData", "file:content/data");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("nx-path", FOLDER_2_FILE);

        testRunner.enqueue("", attributes);
        testRunner.run(1);
        testRunner.assertTransferCount(NuxeoDocumentToAttributes.REL_FAILURE, 0);
        testRunner.assertTransferCount(NuxeoDocumentToAttributes.REL_SUCCESS, 1);

        testRunner.getFlowFilesForRelationship(NuxeoDocumentToAttributes.REL_SUCCESS).stream().forEach(
                doc -> doc.assertAttributeExists("fileData"));
    }

}
