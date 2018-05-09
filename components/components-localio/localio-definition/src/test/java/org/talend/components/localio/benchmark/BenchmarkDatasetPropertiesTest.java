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

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.talend.daikon.properties.presentation.Form.MAIN;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;
import org.talend.daikon.serialize.jsonschema.JsonSchemaUtil;

/**
 * Unit tests for {@link BenchmarkDatasetProperties}.
 */
public class BenchmarkDatasetPropertiesTest {

    /**
     * Instance to test. A new instance is created for each test.
     */
    private BenchmarkDatasetProperties properties = null;

    @Before
    public void setup() {
        properties = new BenchmarkDatasetProperties("test");
        properties.init();
    }

    /**
     * Check the correct default values in the properties.
     */
    @Test
    public void testDefaultProperties() {
        assertThat(properties.generator.getValue(), is(BenchmarkDatasetProperties.GeneratorType.NEXMARK));
    }

    /**
     * Check the setup of the form layout.
     */
    @Test
    public void testSetupLayout() {
        properties.setupLayout();
        Form main = properties.getForm(Form.MAIN);
        assertThat(main, notNullValue());
        assertThat(main.getWidgets(), hasSize(1));

        // Check the widgets.
        Widget w = main.getWidget(properties.generator.getName());
        assertThat(w.getWidgetType(), is(Widget.DEFAULT_WIDGET_TYPE));
        assertThat(w.isVisible(), is(true));
    }

    /**
     * Check the changes to the form.
     */
    @Test
    public void testRefreshLayout() {
        properties.setupLayout();
        Form main = properties.getForm(Form.MAIN);
        assertThat(main, notNullValue());
        assertThat(main.getWidgets(), hasSize(1));

        // Here we can test custom properties for a generator.
    }

    @Test
    public void testJsonSchemaSerialization() throws JSONException {
        String jsonString = JsonSchemaUtil.toJson(properties, MAIN, BenchmarkDatasetDefinition.NAME);
        assertThat(jsonString, notNullValue());

        JSONObject node = new JSONObject(jsonString);
        assertThat(node, notNullValue());
        JSONAssert.assertEquals("{\"jsonSchema\": {},\"uiSchema\": {},\"properties\": {}} ", node, false);

        // Check the properties.
        JSONObject jsonProps = node.getJSONObject("properties");
        JSONAssert.assertEquals("{\"@definitionName\": \"BenchmarkDataset\"} ", jsonProps, false);
    }

}
