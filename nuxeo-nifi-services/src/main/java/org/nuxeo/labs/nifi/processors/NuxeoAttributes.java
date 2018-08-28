package org.nuxeo.labs.nifi.processors;

import org.apache.nifi.components.AllowableValue;

public interface NuxeoAttributes {

    AllowableValue YES = new AllowableValue("true", "Yes", "Yes");

    AllowableValue NO = new AllowableValue("false", "No", "No");

    String ERROR = "nx-error";

    String ENTITY_TYPE = "nx-entity";

    String DOC_ID = "nx-docid";

    String PATH = "nx-path";

    String OPERATION = "nx-op";

    String PROP_KEY_PATTERN = "[_:a-zA-Z][\\w:\\-\\.]*";

}
