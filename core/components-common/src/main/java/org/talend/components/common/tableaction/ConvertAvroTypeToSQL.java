// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.common.tableaction;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.talend.daikon.avro.AvroRegistry;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.avro.SchemaConstants;
import org.talend.daikon.avro.converter.AvroConverter;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class ConvertAvroTypeToSQL {

    private final static Map<Integer, String> SQLTypesMap = new HashMap<>();
    static {
        // Get all field in java.sql.Types
        Field[] fields = java.sql.Types.class.getFields();
        for (int i = 0; i < fields.length; i++) {
            try {
                String name = fields[i].getName();
                Integer value = (Integer) fields[i].get(null);
                SQLTypesMap.put(value, name);
            } catch (IllegalAccessException e) {
            }
        }
    }

    private final static AvroRegistry avroRegistry = new AvroRegistry();

    public final static String convertToSQLTypeString(Schema schema){
        int sqlType = convertToSQLType(schema);
        String sType = SQLTypesMap.get(sqlType);
        if(sType == null){
            throw new UnsupportedOperationException("Can't find "+sqlType+" sql type.");
        }

        return sType;
    }

    public final static int convertToSQLType(Schema schema) {
        Schema.Type type = schema.getType(); // The standard Avro Type
        LogicalType logicalType = schema.getLogicalType(); // The logical type for Data by example
        String javaType = null;  // And the Talend java type if standard Avro type is Union

        if (logicalType == null && type == Schema.Type.UNION) {
            for (Schema s : schema.getTypes()) {
                logicalType = null;
                if (s.getType() != Schema.Type.NULL) {
                    type = s.getType();
                    javaType = s.getProp(SchemaConstants.JAVA_CLASS_FLAG);
                    logicalType = schema.getLogicalType();
                    if (javaType == null && logicalType == null) {
                        type = s.getType(); // Get Avro type if JAVA_CLASS_FLAG is not defined
                    }
                    break;
                }
            }
        }

        int sqlType = Types.NULL;
        if(logicalType != null){
            sqlType = convertAvroLogicialType(logicalType);
        }
        else if (javaType == null) {
            sqlType = convertRawAvroType(type);
        } else {
            sqlType = convertTalendAvroType(javaType);
        }

        return sqlType;
    }

    private static int convertRawAvroType(Schema.Type type) {
        int sqlType = Types.NULL;

        switch (type) {
        case ENUM:
        case RECORD:
        case ARRAY:
        case MAP:
        case FIXED:
        case NULL:
            throw new UnsupportedOperationException(type + "Avro type not supported");
        case STRING:
            sqlType = Types.VARCHAR;
            break;
        case BYTES:
            sqlType = Types.BINARY;
            break;
        case INT:
            sqlType = Types.NUMERIC;
            break;
        case LONG:
            sqlType = Types.NUMERIC;
            break;
        case FLOAT:
            sqlType = Types.FLOAT;
            break;
        case DOUBLE:
            sqlType = Types.DOUBLE;
            break;
        case BOOLEAN:
            sqlType = Types.BOOLEAN;
            break;
        }

        return sqlType;
    }

    private static int convertAvroLogicialType(LogicalType logicalType) {
        int sqlType = Types.NULL;

        if(logicalType == LogicalTypes.timestampMillis()){
            sqlType = Types.TIMESTAMP;
        }
        else if (logicalType instanceof LogicalTypes.Decimal){
            sqlType = Types.DOUBLE;
        }
        else if (logicalType == LogicalTypes.date()){
            sqlType = Types.DATE;
        }
        else if(logicalType == LogicalTypes.uuid()){
            sqlType = Types.VARCHAR;
        }
        else if(logicalType == LogicalTypes.timestampMicros()){
            sqlType = Types.TIMESTAMP;
        }
        else if(logicalType == LogicalTypes.timeMillis()){
            sqlType = Types.NUMERIC;
        }
        else if(logicalType == LogicalTypes.timeMicros()){
            sqlType = Types.NUMERIC;
        }
        else{
            throw new UnsupportedOperationException("Logicial type "+logicalType+" not supported");
        }

        return sqlType;
    }

    private static int convertTalendAvroType(String javaType) {

        int sqlType = Types.NULL;
        if (javaType.equals("java.lang.Byte")) {
            sqlType = Types.SMALLINT;
        } else if (javaType.equals("java.lang.Short")) {
            sqlType = Types.SMALLINT;
        } else if (javaType.equals("java.lang.Character")) {
            sqlType = Types.CHAR;
        } else if (javaType.equals("java.util.Date")) {
            sqlType = Types.DATE;
        } else if (javaType.equals("java.math.BigDecimal")) {
            sqlType = Types.DOUBLE;
        } else {
            throw new UnsupportedOperationException(javaType + " class can't be converted to SQL type.");
        }

        return sqlType;
    }

}