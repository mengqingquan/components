package org.talend.components.jdbc.validation;

public class QueryValidatorFactory {

    public static enum ValidationType {
        PATTERN,
        CALCITE;
    }

    public static QueryValidator createValidator(final ValidationType validationType) {
        switch (validationType) {
        case CALCITE:
            return new CalciteQueryValidator();
        case PATTERN:
            return new PatternQueryValidator();
        default:
            throw new IllegalArgumentException("Unknown validation type " + validationType);
        }
    }

}
