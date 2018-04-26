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

package org.talend.components.simplefileio;

import org.talend.components.common.dataset.DatasetProperties;
import org.talend.components.simplefileio.local.EncodingType;
import org.talend.daikon.properties.PropertiesImpl;
import org.talend.daikon.properties.ReferenceProperties;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.property.Property;
import org.talend.daikon.properties.property.PropertyFactory;

public class SimpleFileIODatasetProperties extends PropertiesImpl implements DatasetProperties<SimpleFileIODatastoreProperties> {

    public Property<SimpleFileIOFormat> format = PropertyFactory.newEnum("format", SimpleFileIOFormat.class).setRequired();

    public Property<String> path = PropertyFactory.newString("path", "").setRequired();

    public Property<RecordDelimiterType> recordDelimiter = PropertyFactory.newEnum("recordDelimiter", RecordDelimiterType.class)
            .setValue(RecordDelimiterType.LF);

    public Property<String> specificRecordDelimiter = PropertyFactory.newString("specificRecordDelimiter", "\\n");

    public Property<FieldDelimiterType> fieldDelimiter = PropertyFactory.newEnum("fieldDelimiter", FieldDelimiterType.class)
            .setValue(FieldDelimiterType.SEMICOLON);

    public Property<String> specificFieldDelimiter = PropertyFactory.newString("specificFieldDelimiter", ";");
    
    public Property<EncodingType> encoding = PropertyFactory.newEnum("encoding", EncodingType.class);
    public Property<Boolean> setHeaderLine = PropertyFactory.newBoolean("setHeaderLine", false);
    public Property<Integer> headerLine = PropertyFactory.newInteger("headerLine", 0);
    
    //advice not set them as default they break the split function for hadoop and beam
    public Property<String> textEnclosureCharacter = PropertyFactory.newString("textEnclosureCharacter", "");
    public Property<String> escapeCharacter = PropertyFactory.newString("escapeCharacter", "");

    public final transient ReferenceProperties<SimpleFileIODatastoreProperties> datastoreRef = new ReferenceProperties<>(
            "datastoreRef", SimpleFileIODatastoreDefinition.NAME);

    public SimpleFileIODatasetProperties(String name) {
        super(name);
    }

    @Override
    public SimpleFileIODatastoreProperties getDatastoreProperties() {
        return datastoreRef.getReference();
    }

    @Override
    public void setDatastoreProperties(SimpleFileIODatastoreProperties datastoreProperties) {
        datastoreRef.setReference(datastoreProperties);
    }

    @Override
    public void setupProperties() {
        super.setupProperties();
        format.setValue(SimpleFileIOFormat.CSV);
    }

    @Override
    public void setupLayout() {
        super.setupLayout();
        Form mainForm = new Form(this, Form.MAIN);
        mainForm.addRow(path);
        mainForm.addRow(format);
        mainForm.addRow(recordDelimiter);
        mainForm.addRow(specificRecordDelimiter);
        mainForm.addRow(fieldDelimiter);
        mainForm.addRow(specificFieldDelimiter);
        
        //CSV properties
        mainForm.addRow(textEnclosureCharacter);
        mainForm.addRow(escapeCharacter);
        mainForm.addRow(encoding);
        mainForm.addRow(setHeaderLine);
        mainForm.addColumn(headerLine);
        
        //Excel TODO
    }

    @Override
    public void refreshLayout(Form form) {
        super.refreshLayout(form);
        // Main properties
        if (form.getName().equals(Form.MAIN)) {
            boolean isCSV = format.getValue() == SimpleFileIOFormat.CSV;
          
            form.getWidget(recordDelimiter).setVisible(isCSV);
            form.getWidget(specificRecordDelimiter).setVisible(
                isCSV && recordDelimiter.getValue().equals(RecordDelimiterType.OTHER));

            form.getWidget(fieldDelimiter).setVisible(isCSV);
            form.getWidget(specificFieldDelimiter).setVisible(
                isCSV && fieldDelimiter.getValue().equals(FieldDelimiterType.OTHER));
            
            form.getWidget(textEnclosureCharacter).setVisible(isCSV);
            form.getWidget(escapeCharacter).setVisible(isCSV);
            form.getWidget(encoding).setVisible(isCSV);
            form.getWidget(setHeaderLine).setVisible(isCSV);
            form.getWidget(headerLine).setVisible(isCSV && setHeaderLine.getValue());
        }
    }

    public void afterFieldDelimiter() {
        refreshLayout(getForm(Form.MAIN));
    }

    public void afterRecordDelimiter() {
        refreshLayout(getForm(Form.MAIN));
    }
    
    public void afterSetHeaderLine() {
        refreshLayout(getForm(Form.MAIN));
    }

    public String getRecordDelimiter() {
        if (RecordDelimiterType.OTHER.equals(recordDelimiter.getValue())) {
            return specificRecordDelimiter.getValue();
        } else {
            return recordDelimiter.getValue().getDelimiter();
        }
    }

    public String getFieldDelimiter() {
        if (FieldDelimiterType.OTHER.equals(fieldDelimiter.getValue())) {
            return specificFieldDelimiter.getValue();
        } else {
            return fieldDelimiter.getValue().getDelimiter();
        }
    }

    public void afterFormat() {
        refreshLayout(getForm(Form.MAIN));
    }

    public enum RecordDelimiterType {
        LF("\n"),
        CR("\r"),
        CRLF("\r\n"),
        OTHER("Other");

        private final String value;

        private RecordDelimiterType(final String value) {
            this.value = value;
        }

        public String getDelimiter() {
            return value;
        }
    }

    public enum FieldDelimiterType {
        SEMICOLON(";"),
        COMMA(","),
        TABULATION("\t"),
        SPACE(" "),
        OTHER("Other");

        private final String value;

        private FieldDelimiterType(final String value) {
            this.value = value;
        }

        public String getDelimiter() {
            return value;
        }
    }
    
    public String getEncoding() {
        return encoding.getValue().name();
    }
  
    public long getHeaderLine() {
      if(setHeaderLine.getValue()) {
          Integer value = headerLine.getValue();
          if(value != null) { 
              return Math.max(0l, value.longValue());
          }
      }
      
      return 0l;
    }
  
    public String getEscapeCharacter() {
      return escapeCharacter.getValue();
    }
  
    public String getTextEnclosureCharacter() {
      return textEnclosureCharacter.getValue();
    }
}
