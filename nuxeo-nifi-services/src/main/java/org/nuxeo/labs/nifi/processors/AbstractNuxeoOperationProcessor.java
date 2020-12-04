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
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.nuxeo.client.objects.Entity;
import org.nuxeo.client.objects.Operation;
import org.nuxeo.client.objects.PaginableEntity;
import org.nuxeo.client.objects.RepositoryEntity;
import org.nuxeo.client.objects.blob.Blob;
import org.nuxeo.client.objects.blob.Blobs;

public abstract class AbstractNuxeoOperationProcessor extends AbstractNuxeoDynamicProcessor {

    public static final PropertyDescriptor OPERATION_ID = new PropertyDescriptor.Builder().name("OPERATION_ID")
                                                                                          .displayName("Operation ID")
                                                                                          .description(
                                                                                                  "Operation ID to use.")
                                                                                          .expressionLanguageSupported(
                                                                                                  ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                          .required(true)
                                                                                          .addValidator(
                                                                                                  StandardValidators.NON_BLANK_VALIDATOR)
                                                                                          .build();

    public static final PropertyDescriptor SPLIT_RESPONSE = new PropertyDescriptor.Builder().name("SPLIT_RESPONSE")
                                                                                            .displayName(
                                                                                                    "Split Response")
                                                                                            .description(
                                                                                                    "Split paginable responses into multiple flow files.")
                                                                                            .allowableValues(YES, NO)
                                                                                            .defaultValue("true")
                                                                                            .required(true)
                                                                                            .addValidator(
                                                                                                    StandardValidators.BOOLEAN_VALIDATOR)
                                                                                            .build();

    protected boolean splitResponse = false;

    public AbstractNuxeoOperationProcessor() {
        super();
    }

    protected void processorScheduled(ProcessContext ctx) {
        super.processorScheduled(ctx);
        this.splitResponse = ctx.getProperty(SPLIT_RESPONSE).asBoolean();
    }

    protected Operation enrichOperation(ProcessContext ctx, FlowFile ff, Operation op) {
        for (PropertyDescriptor desc : this.dynamicProperties) {
            String arg = getArg(ctx, ff, null, desc);
            if (arg != null) {
                op.context(desc.getName(), arg);
            }
        }
        return op;
    }

    protected void executeOperation(ProcessContext ctx, ProcessSession session, FlowFile ff, Operation op) {
        Object obj = op.execute();
        if (obj == null) {
            getLogger().debug("No response from operation: " + op.getOperationId());
            return;
        }
        if (this.splitResponse && obj instanceof PaginableEntity<?>) {
            @SuppressWarnings("unchecked")
            PaginableEntity<Object> page = (PaginableEntity<Object>) obj;
            for (Object ent : page.getEntries()) {
                sendDocument(ctx, session, ff, ent);
            }
        } else if (obj instanceof Blobs) {
            Blobs blobs = (Blobs) obj;
            for (Blob blob : blobs.getBlobs()) {
                sendBlob(ctx, session, ff, blob);
            }
        } else if (obj instanceof Blob) {
            Blob blob = (Blob) obj;
            sendBlob(ctx, session, ff, blob);
        } else if (obj instanceof Entity || obj instanceof RepositoryEntity<?, ?>) {
            sendDocument(ctx, session, ff, obj);
        } else if (obj instanceof String) {
            String data = obj.toString();
            FlowFile childFlow = session.create(ff);
            try (OutputStream out = session.write(childFlow)) {
                IOUtils.write(data, out, UTF8);
            } catch (IOException e) {
                return;
            }
            session.transfer(childFlow, REL_SUCCESS);
        } else if (obj != null) {
            getLogger().warn("Unknown object: " + obj + ", type: " + obj.getClass());
        }
    }

    protected void sendDocument(ProcessContext ctx, ProcessSession session, FlowFile ff, Object doc) {
        FlowFile childFlow = ff == null ? session.create() : session.create(ff);

        // Convert and write to JSON
        String json = nxClient().getConverterFactory().writeJSON(doc);
        try (OutputStream out = session.write(childFlow)) {
            IOUtils.write(json, out, UTF8);
        } catch (IOException e) {
            getLogger().error("Error serializing entity" + doc, e);
            return;
        }
        try {
            Method type = doc.getClass().getMethod("getEntityType");
            String entity = (String) type.invoke(doc);
            session.putAttribute(childFlow, VAR_ENTITY_TYPE, entity);
        } catch (Exception ex) {
            getLogger().error("No entity type", ex);
        }
        session.transfer(childFlow, REL_SUCCESS);
    }

    protected void sendBlob(ProcessContext ctx, ProcessSession session, FlowFile ff, Blob blob) {
        FlowFile childFlow = ff == null ? session.create() : session.create(ff);

        // Copy blob to output
        try (InputStream in = blob.getStream(); OutputStream out = session.write(childFlow)) {
            IOUtils.copy(in, out);
        } catch (IOException e) {
            getLogger().error("Error serializing blob" + blob, e);
            return;
        }
        session.transfer(childFlow, REL_SUCCESS);
    }

}