package org.nuxeo.labs.nifi.processors;

import org.apache.nifi.components.AllowableValue;

public interface NuxeoAttributes {

    AllowableValue YES = new AllowableValue("true", "Yes", "Yes");

    AllowableValue NO = new AllowableValue("false", "No", "No");

    String VAR_BATCH = "nx-batch";

    String VAR_DOC_ID = "nx-docid";

    String VAR_ERROR = "nx-error";

    String VAR_ENTITY_TYPE = "nx-entity";

    String VAR_FILENAME = "filename";

    String VAR_INDEX = "nx-index";

    String VAR_NAME = "nx-name";

    String VAR_OPERATION = "nx-op";

    String VAR_PATH = "nx-path";

    String VAR_TITLE = "nx-title";

    String VAR_TYPE = "nx-type";

    String VAR_XPATH = "nx-xpath";

    String PROP_KEY_PATTERN = "[_:a-zA-Z][\\w:\\-\\.]*";

}
