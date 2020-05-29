package org.chronos.chronograph.internal.impl.schema;

import com.google.common.collect.*;
import com.google.common.collect.Table.Cell;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.schema.SchemaValidationResult;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class SchemaValidationResultImpl implements SchemaValidationResult {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final Table<Element, String, Throwable> table = HashBasedTable.create();

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public SchemaValidationResultImpl() {
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public int getFailureCount() {
        return this.table.size();
    }

    @Override
    public Set<String> getFailedValidators() {
        return Collections.unmodifiableSet(Sets.newHashSet(this.table.columnKeySet()));
    }

    @Override
    public Map<String, List<Pair<Element, Throwable>>> getViolationsByValidators(){
        Map<String, List<Pair<Element, Throwable>>> resultMap = Maps.newHashMap();
        for(Cell<Element, String, Throwable> cell : this.table.cellSet()){
            List<Pair<Element, Throwable>> list = resultMap.computeIfAbsent(cell.getColumnKey(), k -> Lists.newArrayList());
            list.add(Pair.of(cell.getRowKey(), cell.getValue()));
        }
        return resultMap;
    }


    @Override
    public Map<String, Throwable> getFailedValidatorExceptionsForElement(final Element element) {
        return Collections.unmodifiableMap(this.table.row(element));
    }

    @Override
    public String generateErrorMessage() {
        if(this.getFailureCount() <= 0){
            return "No Graph Schema violations were detected.";
        }
        int maxViolationsToShowPerValidator = 5;
        StringBuilder msg = new StringBuilder();
        msg.append("Cannot apply graph commit: ");
        msg.append(this.getFailureCount());
        msg.append(" Graph Schema violations were detected. ");
        msg.append("The transaction will be rolled back, no changes will be applied. ");
        msg.append("The following validations were reported:\n");
        Map<String, List<Pair<Element, Throwable>>> validatorToViolations = this.getViolationsByValidators();
        for (Entry<String, List<Pair<Element, Throwable>>> entry : validatorToViolations.entrySet()) {
            String validatorName = entry.getKey();
            List<Pair<Element, Throwable>> violations = entry.getValue();
            msg.append(" - ");
            msg.append(validatorName);
            msg.append(" (");
            msg.append(violations.size());
            msg.append(" violations):\n");
            violations.stream().limit(maxViolationsToShowPerValidator).forEach(violation -> {
                Element element = violation.getLeft();
                Throwable error = violation.getRight();
                msg.append("\tat ");
                if(element instanceof Vertex){
                    msg.append("Vertex");
                }else if(element instanceof Edge){
                    msg.append("Edge");
                }else {
                    msg.append(element.getClass().getSimpleName());
                }
                msg.append("[");
                msg.append(element.id());
                msg.append("]: ");
                msg.append(error.getMessage());
                msg.append("\n");
            });
            if(violations.size() > maxViolationsToShowPerValidator){
                int more = violations.size() - maxViolationsToShowPerValidator;
                if(more > 0){
                    msg.append("\t... and ");
                    msg.append(more);
                    msg.append(" other elements reported by this validator.");
                }
            }
        }
        return msg.toString();
    }

    @Override
    public String toString() {
        if (this.isSuccess()) {
            return "ValidationResult[SUCCESS]";
        } else {
            return "ValidationResult[FAILURE: " + this.getFailedValidators().stream().collect(Collectors.joining(", ")) + "]";
        }
    }

    // =================================================================================================================
    // INTERNAL API
    // =================================================================================================================

    public void addIssue(Element graphElement, String validatorName, Throwable issue) {
        this.table.put(graphElement, validatorName, issue);
    }


}
