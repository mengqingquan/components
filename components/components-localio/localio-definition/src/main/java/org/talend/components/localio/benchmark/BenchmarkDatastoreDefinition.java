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

import java.net.MalformedURLException;
import java.net.URL;

import org.talend.components.api.component.runtime.DependenciesReader;
import org.talend.components.api.component.runtime.JarRuntimeInfo;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.common.dataset.DatasetProperties;
import org.talend.components.common.datastore.DatastoreDefinition;
import org.talend.components.localio.LocalIOComponentFamilyDefinition;
import org.talend.components.localio.devnull.DevNullOutputDefinition;
import org.talend.daikon.definition.DefinitionImageType;
import org.talend.daikon.definition.I18nDefinition;
import org.talend.daikon.runtime.RuntimeInfo;

/**
 * A datastore that does not require any configuration or persistent resources, used to coordinate the fixed datasets.
 */
public class BenchmarkDatastoreDefinition extends I18nDefinition implements DatastoreDefinition<BenchmarkDatastoreProperties> {

    public static final String RUNTIME = "org.talend.components.localio.runtime.benchmark.BenchmarkDatastoreRuntime";

    public static final String NAME = "BenchmarkDatastore";

    public static final boolean IS_CLASSLOADER_REUSABLE = true;

    public BenchmarkDatastoreDefinition() {
        super(NAME);
    }

    @Override
    public Class<BenchmarkDatastoreProperties> getPropertiesClass() {
        return BenchmarkDatastoreProperties.class;
    }

    @Deprecated
    @Override
    public String getImagePath() {
        return null;
    }

    @Override
    public String getImagePath(DefinitionImageType type) {
        return null;
    }

    @Override
    public String getIconKey() {
        return "clock";
    }

    @Override
    public DatasetProperties createDatasetProperties(BenchmarkDatastoreProperties storeProp) {
        BenchmarkDatasetProperties setProp = new BenchmarkDatasetProperties(BenchmarkDatasetDefinition.NAME);
        setProp.init();
        setProp.setDatastoreProperties(storeProp);
        return setProp;
    }

    @Override
    public String getInputCompDefinitionName() {
        return BenchmarkInputDefinition.NAME;
    }

    @Override
    public String getOutputCompDefinitionName() {
        // There is no output component for this datastore.
        return null;
    }

    @Override
    public RuntimeInfo getRuntimeInfo(BenchmarkDatastoreProperties properties) {
        try {
            return new JarRuntimeInfo(new URL(LocalIOComponentFamilyDefinition.MAVEN_DEFAULT_RUNTIME_URI),
                    DependenciesReader.computeDependenciesFilePath(LocalIOComponentFamilyDefinition.MAVEN_GROUP_ID,
                            LocalIOComponentFamilyDefinition.MAVEN_DEFAULT_RUNTIME_ARTIFACT_ID),
                    RUNTIME, IS_CLASSLOADER_REUSABLE);
        } catch (MalformedURLException e) {
            throw new ComponentException(e);
        }
    }
}
