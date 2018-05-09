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

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import org.junit.Assert;
import org.junit.Test;
import org.talend.components.api.component.ComponentDefinition;
import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.component.runtime.ExecutionEngine;
import org.talend.daikon.definition.DefinitionImageType;
import org.talend.daikon.exception.TalendRuntimeException;
import org.talend.daikon.runtime.RuntimeInfo;

/**
 * Unit tests for {@link BenchmarkInputDefinition}.
 */
public class BenchmarkInputDefinitionTest {

    /**
     * Instance to test. Definitions are immutable.
     */
    private final ComponentDefinition def = new BenchmarkInputDefinition();

    /**
     * Checks the basic attributes of the definition.
     */
    @Test
    public void testBasic() {
        Assert.assertThat(def.getName(), is("BenchmarkInput"));
        Assert.assertThat(def.getFamilies(), arrayContaining("LocalIO"));
        Assert.assertThat(def.getPropertiesClass(), is(equalTo((Object) BenchmarkInputProperties.class)));
        Assert.assertThat(def.getImagePath(DefinitionImageType.PALETTE_ICON_32X32), is("BenchmarkInput_icon32.png"));
        Assert.assertThat(def.getIconKey(), is("clock"));
        Assert.assertThat(def.getSupportedConnectorTopologies(), contains(ConnectorTopology.OUTGOING));
    }

    /**
     * Checks the {@link RuntimeInfo} of the definition.
     */
    @Test
    public void testRuntimeInfo() {
        RuntimeInfo runtimeInfo = def.getRuntimeInfo(ExecutionEngine.BEAM, null, ConnectorTopology.OUTGOING);
        Assert.assertThat(runtimeInfo.getRuntimeClassName(),
                is("org.talend.components.localio.runtime.benchmark.BenchmarkInputRuntime"));
        // Other runtime information is not available until the runtime module is built and installed.
    }

    @Test(expected = TalendRuntimeException.class)
    public void testFailRuntimeInfoIncoming() {
        def.getRuntimeInfo(ExecutionEngine.BEAM, null, ConnectorTopology.INCOMING);
    }

    @Test(expected = TalendRuntimeException.class)
    public void testFailRuntimeInfoIncomingAndOutgoing() {
        def.getRuntimeInfo(ExecutionEngine.BEAM, null, ConnectorTopology.INCOMING_AND_OUTGOING);
    }

    @Test(expected = TalendRuntimeException.class)
    public void testFailRuntimeInfoNone() {
        def.getRuntimeInfo(ExecutionEngine.BEAM, null, ConnectorTopology.NONE);
    }

    @Test(expected = TalendRuntimeException.class)
    public void testFailRuntimeInfoDi() {
        def.getRuntimeInfo(ExecutionEngine.DI, null, ConnectorTopology.OUTGOING);
    }

    @Test(expected = TalendRuntimeException.class)
    public void testFailRuntimeInfoSparkBatch() {
        def.getRuntimeInfo(ExecutionEngine.DI_SPARK_BATCH, null, ConnectorTopology.OUTGOING);
    }

    @Test(expected = TalendRuntimeException.class)
    public void testFailRuntimeInfoSparkStreaming() {
        def.getRuntimeInfo(ExecutionEngine.DI_SPARK_STREAMING, null, ConnectorTopology.OUTGOING);
    }
}
