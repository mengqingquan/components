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

import org.talend.components.common.dataset.DatasetProperties;
import org.talend.daikon.properties.PropertiesImpl;
import org.talend.daikon.properties.ReferenceProperties;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.property.PropertyFactory;

/**
 * The benchmark dataset contains a set of records constructed from a benchmark generator.
 */
public class BenchmarkDatasetProperties extends PropertiesImpl implements DatasetProperties<BenchmarkDatastoreProperties> {

    public Property<GeneratorType> generator = PropertyFactory.newEnum("generator", GeneratorType.class).setRequired();

    public final transient ReferenceProperties<BenchmarkDatastoreProperties> datastoreRef = new ReferenceProperties<>(
            "datastoreRef", BenchmarkDatastoreDefinition.NAME);

    public BenchmarkDatasetProperties(String name) {
        super(name);
    }

    @Override
    public BenchmarkDatastoreProperties getDatastoreProperties() {
        return datastoreRef.getReference();
    }

    @Override
    public void setDatastoreProperties(BenchmarkDatastoreProperties datastoreProperties) {
        datastoreRef.setReference(datastoreProperties);
    }

    @Override
    public void setupProperties() {
        super.setupProperties();
        generator.setValue(GeneratorType.NEXMARK);
    }

    @Override
    public void setupLayout() {
        super.setupLayout();
        Form mainForm = new Form(this, Form.MAIN);
        mainForm.addRow(generator);
    }

    /**
     * The known list of generators.
     */
    public enum GeneratorType {
        NEXMARK
    }
}
