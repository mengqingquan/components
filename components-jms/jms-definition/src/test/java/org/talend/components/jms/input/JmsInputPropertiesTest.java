// ============================================================================
//
// Copyright (C) 2006-2015 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================

package org.talend.components.jms.input;

import org.junit.Test;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;

import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class JmsInputPropertiesTest {

    /**
     * Checks {@link JmsInputProperties} sets correctly initial schema
     * properties
     */
    @Test
    public void testDefaultProperties() {
        JmsInputProperties properties = new JmsInputProperties("test");
        assertNull(properties.main.schema.getValue());
        assertEquals("",properties.from.getValue());
        assertEquals(-1,(long)properties.timeout.getValue());
        assertEquals(-1,(long)properties.max_msg.getValue());
        assertEquals("",properties.msg_selector.getValue());
    }

    /**
     * Checks {@link JmsInputProperties} sets correctly initial layout
     * properties
     */
    @Test
    public void testSetupLayout() {
        JmsInputProperties properties = new JmsInputProperties("test");
        properties.main.init();

        properties.setupLayout();

        Form main = properties.getForm(Form.MAIN);
        assertThat(main, notNullValue());

        Collection<Widget> mainWidgets = main.getWidgets();
        assertThat(mainWidgets, hasSize(5));
        Widget mainWidget = main.getWidget("main");
        assertThat(mainWidget, notNullValue());
        Widget from = main.getWidget("from");
        assertThat(from, notNullValue());
        Widget timeout = main.getWidget("timeout");
        assertThat(timeout, notNullValue());
        Widget max_msg = main.getWidget("max_msg");
        assertThat(max_msg, notNullValue());
        Widget msg_selector = main.getWidget("msg_selector");
        assertThat(msg_selector, notNullValue());
    }

    /**
     * Checks {@link JmsInputProperties} sets correctly layout after refresh
     * properties
     */
    @Test
    public void testRefreshLayout() {
        //TODO FIX ISSUE
        /*JmsInputProperties properties = new JmsInputProperties("test");
        properties.main.init();
        System.out.println(properties.toString());
        properties.refreshLayout(properties.getForm(Form.MAIN));

        assertFalse(properties.getForm(Form.MAIN).getWidget("from").isHidden());
        assertFalse(properties.getForm(Form.MAIN).getWidget("timeout").isHidden());
        assertFalse(properties.getForm(Form.MAIN).getWidget("max_msg").isHidden());
        assertFalse(properties.getForm(Form.MAIN).getWidget("msg_selector").isHidden());
*/
    }
}
