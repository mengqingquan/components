// ==============================================================================
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
// ==============================================================================
package org.talend.components.service.rest.mock;

import org.talend.components.common.dataset.DatasetProperties;
import org.talend.daikon.properties.PropertiesImpl;
import org.talend.daikon.properties.ValidationResult;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.property.PropertyFactory;
import org.talend.daikon.serialize.PostDeserializeSetup;
import org.talend.daikon.serialize.migration.SerializeSetVersion;

/**
 * Mock dataset properties for tests.
 */
public class MockDatasetProperties extends PropertiesImpl implements DatasetProperties<MockDatastoreProperties>, SerializeSetVersion {

    public Property<String> tag = PropertyFactory.newString("tag");

    public Property<Integer> tagId = PropertyFactory.newInteger("tagId");
    
    public Property<Boolean> newTag = PropertyFactory.newBoolean("newTag", true);

    /**
     * Default constructor.
     *
     * @param name the properties name.
     */
    public MockDatasetProperties(String name) {
        super(name);
    }

    @Override
    public void setupLayout() {
        super.setupLayout();
        Form mainForm = new Form(this, Form.MAIN);
        mainForm.addColumn(tag);
        mainForm.addColumn(tagId);
        mainForm.addColumn(newTag);
    }

    public ValidationResult validateTag() {
        return new ValidationResult();
    }

    public ValidationResult validateTagId() {
        return new ValidationResult(ValidationResult.Result.OK, "tagId is OK");
    }

    @Override
    public MockDatastoreProperties getDatastoreProperties() {
        return null;
    }

    @Override
    public void setDatastoreProperties(MockDatastoreProperties datastoreProperties) {

    }

    @Override
    public int getVersionNumber() {
      return 1;
    }
    
    @Override
    public boolean postDeserialize(int version, PostDeserializeSetup setup, boolean persistent) {
        boolean migrated = super.postDeserialize(version, setup, persistent);

        if(version < this.getVersionNumber()) {
            migrated = true;
            this.newTag.setValue(false);
        }

        return migrated;
    }
    
}
