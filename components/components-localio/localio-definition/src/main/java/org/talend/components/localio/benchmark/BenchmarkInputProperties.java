// ============================================================================
//
// Copyright (C) 2006-2017 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.localio.benchmark;

import java.util.HashSet;
import java.util.Set;

import org.talend.components.api.component.Connector;
import org.talend.components.api.component.PropertyPathConnector;
import org.talend.components.common.FixedConnectorsComponentProperties;
import org.talend.components.common.SchemaProperties;
import org.talend.components.common.io.IOProperties;
import org.talend.daikon.properties.ReferenceProperties;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.property.PropertyFactory;

/**
 * Configures a fixed input component.
 */
public class BenchmarkInputProperties extends FixedConnectorsComponentProperties
        implements IOProperties<BenchmarkDatasetProperties> {

    public BenchmarkInputProperties(String name) {
        super(name);
    }

    public transient PropertyPathConnector OUT_CONNECTOR = new PropertyPathConnector(Connector.MAIN_NAME, "outgoing");

    /** Property used to specify that this component generates unbounded input. */
    public Property<Boolean> isStreaming = PropertyFactory.newBoolean("isStreaming", false);

    public SchemaProperties outgoing = new SchemaProperties("outgoing");

    public Property<Boolean> useSeed = PropertyFactory.newBoolean("useSeed", false);

    /** The specific random seed for generating the data. */
    public Property<Long> seed = PropertyFactory.newProperty(Long.class, "seed");

    public Property<Boolean> useMaxReadTime = PropertyFactory.newBoolean("useMaxReadTime", false);

    // Max duration(Millions) from start receiving
    public Property<Long> maxReadTime = PropertyFactory.newProperty(Long.class, "maxReadTime");

    public Property<Boolean> useMaxNumRecords = PropertyFactory.newBoolean("useMaxNumRecords", false);

    public Property<Long> maxNumRecords = PropertyFactory.newProperty(Long.class, "maxNumRecords");

    public transient ReferenceProperties<BenchmarkDatasetProperties> datasetRef = new ReferenceProperties<>("datasetRef",
            BenchmarkDatasetDefinition.NAME);

    @Override
    public void setupLayout() {
        super.setupLayout();
        Form mainForm = new Form(this, Form.MAIN);
        mainForm.addRow(isStreaming);
        mainForm.addRow(useSeed);
        mainForm.addRow(seed);
        mainForm.addRow(useMaxReadTime);
        mainForm.addRow(maxReadTime);
        mainForm.addRow(useMaxNumRecords);
        mainForm.addRow(maxNumRecords);
        maxReadTime.setValue(600000L);
        maxNumRecords.setValue(5000L);
    }

    @Override
    public void refreshLayout(Form form) {
        super.refreshLayout(form);
        // Main properties
        if (form.getName().equals(Form.MAIN)) {
            // Only add the override values action if the hidden property is set to true.
            form.getWidget(seed).setVisible(useSeed.getValue());
            form.getWidget(useMaxReadTime).setVisible(isStreaming.getValue());
            form.getWidget(maxReadTime).setVisible(isStreaming.getValue() && useMaxReadTime.getValue());
            form.getWidget(useMaxNumRecords).setVisible(isStreaming.getValue());
            form.getWidget(maxNumRecords).setHidden(isStreaming.getValue() && !useMaxNumRecords.getValue());
        }
    }

    @Override
    public BenchmarkDatasetProperties getDatasetProperties() {
        return datasetRef.getReference();
    }

    @Override
    public void setDatasetProperties(BenchmarkDatasetProperties datasetProperties) {
        datasetRef.setReference(datasetProperties);
    }

    @Override
    protected Set<PropertyPathConnector> getAllSchemaPropertiesConnectors(boolean isOutputConnection) {
        HashSet<PropertyPathConnector> connectors = new HashSet<PropertyPathConnector>();
        if (isOutputConnection) {
            // output schema
            connectors.add(OUT_CONNECTOR);
        }
        return connectors;
    }

    public void afterUseSeed() {
        refreshLayout(getForm(Form.MAIN));
    }

    public void afterIsStreaming() {
        refreshLayout(getForm(Form.MAIN));
    }

    public void afterUseMaxReadTime() {
        refreshLayout(getForm(Form.MAIN));
    }

    public void afterUseMaxNumRecords() {
        refreshLayout(getForm(Form.MAIN));
    }

}
