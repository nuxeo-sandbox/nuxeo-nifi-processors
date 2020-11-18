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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.nuxeo.client.objects.Repository;
import org.nuxeo.client.objects.workflow.Workflow;
import org.nuxeo.client.spi.NuxeoClientException;

@Tags({ "nuxeo", "cancel", "workflow" })
@CapabilityDescription("Cancel a workflow instance for a document within Nuxeo.")
@SeeAlso({ GetNuxeoWorkflows.class })
@ReadsAttributes({ @ReadsAttribute(attribute = "nx-workflow", description = "Workflow to cancel."),
        @ReadsAttribute(attribute = NuxeoAttributes.VAR_DOC_ID, description = "Document ID to use if the path isn't specified"),
        @ReadsAttribute(attribute = NuxeoAttributes.VAR_PATH, description = "Path to use, nx-docid overrides") })
@WritesAttributes({ @WritesAttribute(attribute = "nx-workflow-id", description = "ID of document workflow instance."),
        @WritesAttribute(attribute = NuxeoAttributes.VAR_ERROR, description = "Error set if problem occurs") })
@InputRequirement(Requirement.INPUT_ALLOWED)
public class CancelNuxeoWorkflow extends AbstractNuxeoProcessor {

    public static final PropertyDescriptor WORKFLOW = new PropertyDescriptor.Builder().name("WORKFLOW")
                                                                                      .displayName("Workflow")
                                                                                      .description(
                                                                                              "Workflow name to cancel.")
                                                                                      .expressionLanguageSupported(
                                                                                              ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                      .required(true)
                                                                                      .addValidator(Validator.VALID)
                                                                                      .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(NUXEO_CLIENT_SERVICE);
        descriptors.add(TARGET_REPO);
        descriptors.add(WORKFLOW);
        descriptors.add(DOC_PATH);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String workflowName = getArg(context, flowFile, "nx-workflow", WORKFLOW);

        try {
            // Invoke document operation
            Repository rep = getRepository(context);
            List<Workflow> wfs = getDocument(context, flowFile).fetchWorkflowInstances()
                                                               .streamEntries()
                                                               .filter(w -> workflowName.contentEquals(w.getName()))
                                                               .collect(Collectors.toList());

            // Write documents to flowfile
            for (Workflow wf : wfs) {
                rep.cancelWorkflowInstance(wf.getId());
                FlowFile childFlow = session.create(flowFile);
                session.putAttribute(childFlow, VAR_ENTITY_TYPE, wf.getEntityType());
                session.putAttribute(childFlow, "nx-workflow-id", wf.getId());

                // Convert and write to JSON
                String json = this.nuxeoClient.getConverterFactory().writeJSON(wf);
                try (OutputStream out = session.write(childFlow)) {
                    IOUtils.write(json, out, UTF8);
                } catch (IOException e) {
                    session.putAttribute(flowFile, VAR_ERROR, e.getMessage());
                    session.transfer(flowFile, REL_FAILURE);
                    continue;
                }
                session.transfer(childFlow, REL_SUCCESS);
            }
        } catch (NuxeoClientException nce) {
            session.putAttribute(flowFile, VAR_ERROR, nce.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
