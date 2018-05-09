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
 * Unit tests for {@link BenchmarkInputProperties}.
 */
public class BenchmarkInputPropertiesTest {

    /**
     * Instance to test. A new instance is created for each test.
     */
    private BenchmarkInputProperties properties = null;

    @Before
    public void setup() {
        properties = new BenchmarkInputProperties("test");
        properties.init();
    }

    /**
     * Check the correct default values in the properties.
     */
    @Test
    public void testDefaultProperties() {
        assertThat(properties.isStreaming.getValue(), is(false));
        assertThat(properties.useMaxNumRecords.getValue(), is(false));
        assertThat(properties.maxNumRecords.getValue(), is(5000L));
        assertThat(properties.useMaxReadTime.getValue(), is(false));
        assertThat(properties.maxReadTime.getValue(), is(600000L));
    }

    /**
     * Check the setup of the form layout.
     */
    @Test
    public void testSetupLayout() {
        properties.setupLayout();
        Form main = properties.getForm(Form.MAIN);
        assertThat(main, notNullValue());
        assertThat(main.getWidgets(), hasSize(7));
        // Check the widgets.
        Widget w = main.getWidget(properties.isStreaming.getName());
        assertThat(w.getWidgetType(), is(Widget.DEFAULT_WIDGET_TYPE));
        assertThat(w.isVisible(), is(true));
        w = main.getWidget(properties.useSeed.getName());
        assertThat(w.getWidgetType(), is(Widget.DEFAULT_WIDGET_TYPE));
        assertThat(w.isVisible(), is(true));
        w = main.getWidget(properties.seed.getName());
        assertThat(w.getWidgetType(), is(Widget.DEFAULT_WIDGET_TYPE));
        assertThat(w.isVisible(), is(false));
        w = main.getWidget(properties.useMaxNumRecords.getName());
        assertThat(w.getWidgetType(), is(Widget.DEFAULT_WIDGET_TYPE));
        assertThat(w.isVisible(), is(false));
        w = main.getWidget(properties.maxNumRecords.getName());
        assertThat(w.getWidgetType(), is(Widget.DEFAULT_WIDGET_TYPE));
        assertThat(w.isVisible(), is(true));
        w = main.getWidget(properties.useMaxReadTime.getName());
        assertThat(w.getWidgetType(), is(Widget.DEFAULT_WIDGET_TYPE));
        assertThat(w.isVisible(), is(false));
        w = main.getWidget(properties.maxReadTime.getName());
        assertThat(w.getWidgetType(), is(Widget.DEFAULT_WIDGET_TYPE));
        assertThat(w.isVisible(), is(false));
    }

    /**
     * Check the changes to the form.
     */
    @Test
    public void testRefreshLayout() {
        properties.setupLayout();
        Form main = properties.getForm(Form.MAIN);
        assertThat(main, notNullValue());
        assertThat(main.getWidgets(), hasSize(7));

        // Turn the override values on.
        properties.isStreaming.setValue(true);
        properties.refreshLayout(main);
        assertThat(main.getWidget(properties.isStreaming.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.useSeed.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.seed.getName()).isVisible(), is(false));
        assertThat(main.getWidget(properties.useMaxNumRecords.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.maxNumRecords.getName()).isVisible(), is(false));
        assertThat(main.getWidget(properties.useMaxReadTime.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.maxReadTime.getName()).isVisible(), is(false));

        properties.useSeed.setValue(true);
        properties.refreshLayout(main);
        assertThat(main.getWidget(properties.isStreaming.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.useSeed.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.seed.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.useMaxNumRecords.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.maxNumRecords.getName()).isVisible(), is(false));
        assertThat(main.getWidget(properties.useMaxReadTime.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.maxReadTime.getName()).isVisible(), is(false));

        properties.useMaxNumRecords.setValue(true);
        properties.refreshLayout(main);
        assertThat(main.getWidget(properties.isStreaming.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.useSeed.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.seed.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.useMaxNumRecords.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.maxNumRecords.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.useMaxReadTime.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.maxReadTime.getName()).isVisible(), is(false));

        properties.useMaxReadTime.setValue(true);
        properties.refreshLayout(main);
        assertThat(main.getWidget(properties.isStreaming.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.useSeed.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.seed.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.useMaxNumRecords.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.maxNumRecords.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.useMaxReadTime.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.maxReadTime.getName()).isVisible(), is(true));

        properties.isStreaming.setValue(false);
        properties.refreshLayout(main);
        assertThat(main.getWidget(properties.isStreaming.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.useSeed.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.seed.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.useMaxNumRecords.getName()).isVisible(), is(false));
        // Note: you can see the number of records desired when streaming is disabled.
        assertThat(main.getWidget(properties.maxNumRecords.getName()).isVisible(), is(true));
        assertThat(main.getWidget(properties.useMaxReadTime.getName()).isVisible(), is(false));
        assertThat(main.getWidget(properties.maxReadTime.getName()).isVisible(), is(false));
    }

    @Test
    public void testJsonSchemaSerialization() throws JSONException {
        String jsonString = JsonSchemaUtil.toJson(properties, MAIN, BenchmarkInputDefinition.NAME);
        assertThat(jsonString, notNullValue());

        JSONObject node = new JSONObject(jsonString);
        assertThat(node, notNullValue());
        JSONAssert.assertEquals("{\"jsonSchema\": {},\"uiSchema\": {},\"properties\": {}} ", node, false);

        // Check the properties.
        JSONObject jsonProps = node.getJSONObject("properties");
        JSONAssert.assertEquals("{\"@definitionName\": \"BenchmarkInput\",\"outgoing\": {},\"isStreaming\": false} ", jsonProps,
                false);
    }

}