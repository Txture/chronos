package org.chronos.chronodb.internal.impl.dump.meta;

public class SchemaValidatorMetadata {

    private String validatorName;
    private String validatorScriptContent;

    public SchemaValidatorMetadata() {

    }

    public SchemaValidatorMetadata(String validatorName, String validatorScriptContent) {
        this.validatorName = validatorName;
        this.validatorScriptContent = validatorScriptContent;
    }

    public String getValidatorName() {
        return validatorName;
    }

    public void setValidatorName(final String validatorName) {
        this.validatorName = validatorName;
    }

    public String getValidatorScriptContent() {
        return validatorScriptContent;
    }

    public void setValidatorScriptContent(final String validatorScriptContent) {
        this.validatorScriptContent = validatorScriptContent;
    }
}
