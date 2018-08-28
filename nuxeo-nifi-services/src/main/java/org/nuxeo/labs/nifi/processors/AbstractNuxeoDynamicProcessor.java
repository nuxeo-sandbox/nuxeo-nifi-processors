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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;

public abstract class AbstractNuxeoDynamicProcessor extends AbstractNuxeoProcessor {

    protected List<PropertyDescriptor> dynamicProperties;

    public AbstractNuxeoDynamicProcessor() {
        super();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder().name(propertyDescriptorName)
                                               .displayName(propertyDescriptorName)
                                               .description(propertyDescriptorName)
                                               .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                               .addValidator(Validator.VALID)
                                               .build();
    }

    protected List<PropertyDescriptor> getDynamicProperties(ProcessContext ctx) {
        Map<PropertyDescriptor, String> props = ctx.getProperties();
        return props.keySet().stream().filter(desc -> desc.isDynamic()).collect(Collectors.toList());
    }

    protected void processorScheduled(ProcessContext ctx) {
        super.processorScheduled(ctx);
        this.dynamicProperties = getDynamicProperties(ctx);
    }

}