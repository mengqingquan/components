package org.talend.components.salesforce;

import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.api.properties.presentation.Form;
import org.talend.components.api.wizard.ComponentWizard;
import org.talend.components.api.wizard.ComponentWizardDefinition;

/**
 *
 */
public class SalesforceConnectionWizard extends ComponentWizard {

    SalesforceModuleListProperties mProps;

    SalesforceConnectionWizard(ComponentWizardDefinition def, String repositoryLocation) {
        super(def, repositoryLocation);

        SalesforceConnectionProperties cProps = new SalesforceConnectionProperties();
        cProps.init();
        addForm(cProps.getForm(SalesforceConnectionProperties.FORM_WIZARD));

        mProps = new SalesforceModuleListProperties().setConnection(cProps).setRepositoryLocation(getRepositoryLocation());
        mProps.init();
        addForm(mProps.getForm(Form.MAIN));
    }

    public boolean supportsProperties(ComponentProperties properties) {
        if (properties instanceof SalesforceConnectionProperties)
            return true;
        return false;
    }

    public void setupProperties(SalesforceConnectionProperties cProps) {
        // Update the connection form properties
        getForms().get(0).setProperties(cProps);
        mProps.setConnection(cProps);
    }

}
