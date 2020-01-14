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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.nuxeo.client.objects.Document;
import org.nuxeo.client.objects.Operation;
import org.nuxeo.client.objects.operation.DocRef;
import org.nuxeo.client.spi.NuxeoClientException;

@Tags({ "nuxeo", "operation", "execution", "document" })
@CapabilityDescription("Execute an operation with a Document in Nuxeo.")
@SeeAlso({ StartNuxeoWorkflow.class })
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = NuxeoAttributes.VAR_DOC_ID, description = "Document ID") })
public class NuxeoDocumentOperation extends AbstractNuxeoOperationProcessor {

    public NuxeoDocumentOperation() {
        super();
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(NUXEO_CLIENT_SERVICE);
        descriptors.add(TARGET_REPO);
        descriptors.add(DOC_PATH);
        descriptors.add(OPERATION_ID);
        descriptors.add(SPLIT_RESPONSE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // Evaluate target operation
        String opId = getArg(context, flowFile, VAR_OPERATION, OPERATION_ID);

        try {
            Document doc = getDocument(context, flowFile);
            Operation op = getClient(context).operation(opId);
            op.input(new DocRef(doc.getId()));
            enrichOperation(context, flowFile, op);
            executeOperation(context, session, flowFile, op);
            session.transfer(flowFile, REL_ORIGINAL);
        } catch (NuxeoClientException nce) {
            getLogger().error("Failed to invoke Document operation: " + opId, nce);
            session.putAttribute(flowFile, VAR_ERROR, String.valueOf(nce));
            session.transfer(flowFile, REL_FAILURE);
        }
    }

}
