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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
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
import org.nuxeo.client.objects.Documents;
import org.nuxeo.client.objects.Repository;
import org.nuxeo.client.spi.NuxeoClientException;

@Tags({ "nuxeo", "get", "children" })
@CapabilityDescription("Get the children documents of a given folderish path.")
@SeeAlso({ GetNuxeoDocument.class })
@ReadsAttributes({
        @ReadsAttribute(attribute = NuxeoAttributes.VAR_DOC_ID, description = "Document ID to use if the path isn't specified"),
        @ReadsAttribute(attribute = NuxeoAttributes.VAR_PATH, description = "Path to use, nx-docid overrides") })
@WritesAttributes({
        @WritesAttribute(attribute = NuxeoAttributes.VAR_DOC_ID, description = "Added for each document retreived"),
        @WritesAttribute(attribute = NuxeoAttributes.VAR_ERROR, description = "Error set if problem occurs") })
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_ALLOWED)
public class GetNuxeoChildren extends AbstractNuxeoProcessor {

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(NUXEO_CLIENT_SERVICE);
        descriptors.add(TARGET_REPO);
        descriptors.add(DOC_PATH);
        descriptors.add(FILTER_SCHEMAS);
        descriptors.add(EMPTY_INPUT);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null && !context.getProperty(EMPTY_INPUT).asBoolean()) {
            return;
        }

        // Get target path
        String docId = getArg(context, flowFile, VAR_DOC_ID, null);
        String path = getArg(context, flowFile, VAR_PATH, DOC_PATH);

        if (StringUtils.isBlank(docId) && StringUtils.isBlank(path)) {
            return;
        }

        try {
            // Invoke document operation
            Repository rep = getRepository(context, flowFile);
            Documents docs = docId != null ? rep.fetchChildrenById(docId) : rep.fetchChildrenByPath(path);

            // Write documents to flowfile
            for (Document doc : docs.getDocuments()) {
                FlowFile childFlow = flowFile == null ? session.create() : session.create(flowFile);
                session.putAttribute(childFlow, VAR_ENTITY_TYPE, doc.getEntityType());
                session.putAttribute(childFlow, VAR_DOC_ID, doc.getId());

                // Convert and write to JSON
                String json = nxClient().getConverterFactory().writeJSON(doc);
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
            if (flowFile == null) {
                flowFile = session.create();
            }
            FlowFile err = session.create(flowFile);
            session.putAttribute(err, VAR_ERROR, nce.getMessage());
            session.transfer(err, REL_FAILURE);
        }
        session.transfer(flowFile, REL_ORIGINAL);
    }
}
