package org.talend.components.localio.benchmark;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.talend.daikon.properties.presentation.Form.MAIN;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.serialize.jsonschema.JsonSchemaUtil;

/**
 * Unit test for {@link BenchmarkDatastoreProperties}
 */
public class BenchmarkDatastorePropertiesTest {

    /**
     * Instance to test. A new instance is created for each test.
     */
    BenchmarkDatastoreProperties properties = null;

    @Before
    public void setup() {
        properties = new BenchmarkDatastoreProperties("test");
        properties.init();
    }

    /**
     * Check the setup of the form layout.
     */
    @Test
    public void testSetupLayout() {
        properties.setupLayout();
        Form main = properties.getForm(Form.MAIN);
        assertThat(main, notNullValue());
        assertThat(main.getWidgets(), hasSize(0));
    }

    @Test
    public void testJsonSchemaSerialization() throws JSONException {
        String jsonString = JsonSchemaUtil.toJson(properties, MAIN, BenchmarkDatastoreDefinition.NAME);
        assertThat(jsonString, notNullValue());

        JSONObject node = new JSONObject(jsonString);
        assertThat(node, notNullValue());
        JSONAssert.assertEquals("{\"jsonSchema\": {},\"uiSchema\": {},\"properties\": {}} ", node, false);

        // Check the properties.
        JSONObject jsonProps = node.getJSONObject("properties");
        JSONAssert.assertEquals("{\"@definitionName\": \"BenchmarkDatastore\"} ", jsonProps, false);
    }
}