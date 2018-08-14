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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
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
import org.nuxeo.client.objects.workflow.Workflow;
import org.nuxeo.client.objects.workflow.Workflows;
import org.nuxeo.client.spi.NuxeoClientException;

@Tags({ "nuxeo", "get", "workflows" })
@CapabilityDescription("Get active workflows for documents within Nuxeo.")
@SeeAlso({})
@ReadsAttributes({
        @ReadsAttribute(attribute = "nx-docid", description = "Document ID to use if the path isn't specified"),
        @ReadsAttribute(attribute = "nx-path", description = "Path to use, nx-docid overrides") })
@WritesAttributes({ @WritesAttribute(attribute = "nx-wfid", description = "ID of document workflow instance."),
        @WritesAttribute(attribute = "nx-error", description = "Error set if problem occurs") })
public class GetNuxeoWorkflows extends AbstractNuxeoProcessor {

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(NUXEO_CLIENT_SERVICE);
        descriptors.add(TARGET_REPO);
        descriptors.add(TARGET_PATH);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            // Invoke document operation
            Workflows wfs = getDocument(context, flowFile).fetchWorkflowInstances();

            // Write documents to flowfile
            for (Workflow wf : wfs.getWorkflows()) {
                FlowFile childFlow = session.create(flowFile);

                // Convert and write to JSON
                String json = this.nuxeoClient.getConverterFactory().writeJSON(wf);
                try (OutputStream out = session.write(childFlow)) {
                    IOUtils.write(json, out, Charset.forName("UTF-8"));
                } catch (IOException e) {
                    continue;
                }
                session.putAttribute(childFlow, "nx-wfid", wf.getId());
                session.transfer(childFlow, REL_SUCCESS);
            }
        } catch (NuxeoClientException nce) {
            session.putAttribute(flowFile, "nx-error", nce.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
