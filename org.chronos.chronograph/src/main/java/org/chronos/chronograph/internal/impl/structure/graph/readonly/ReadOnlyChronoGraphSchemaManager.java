package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import org.chronos.chronograph.api.schema.ChronoGraphSchemaManager;
import org.chronos.chronograph.api.schema.SchemaValidationResult;
import org.chronos.chronograph.api.structure.ChronoElement;

import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class ReadOnlyChronoGraphSchemaManager implements ChronoGraphSchemaManager {

    private ChronoGraphSchemaManager manager;

    public ReadOnlyChronoGraphSchemaManager(ChronoGraphSchemaManager manager) {
        checkNotNull(manager, "Precondition violation - argument 'manager' must not be NULL!");
        this.manager = manager;
    }


    @Override
    public boolean addOrOverrideValidator(final String validatorName, final String scriptContent) {
        return this.unsupportedOperation();
    }

    @Override
    public boolean addOrOverrideValidator(final String validatorName, final String scriptContent, final Object commitMetadata) {
        return this.unsupportedOperation();
    }

    @Override
    public boolean removeValidator(final String validatorName) {
        return this.unsupportedOperation();
    }

    @Override
    public boolean removeValidator(final String validatorName, final Object commitMetadata) {
        return this.unsupportedOperation();
    }

    @Override
    public String getValidatorScript(final String validatorName) {
        return this.manager.getValidatorScript(validatorName);
    }

    @Override
    public Set<String> getAllValidatorNames() {
        return this.manager.getAllValidatorNames();
    }

    @Override
    public SchemaValidationResult validate(final String branch, final Iterable<? extends ChronoElement> elements) {
        return this.manager.validate(branch, elements);
    }

    private <T> T unsupportedOperation() {
        throw new UnsupportedOperationException("This operation is not supported on a read-only graph!");
    }
}
