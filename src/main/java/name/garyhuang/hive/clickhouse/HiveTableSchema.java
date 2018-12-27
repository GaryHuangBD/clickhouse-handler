package name.garyhuang.hive.clickhouse;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author GaryHuang
 * @date 2018/12/27
 **/
public class HiveTableSchema {

    private static List<Text> colNames = Lists.newArrayList();
    private static List<String> fieldNames;
    private static StructTypeInfo typeInfo;
    private static ObjectInspector inspector;
    private static List<TypeInfo> colTypes;

    public static void init(Properties tblProperties) {
        fieldNames = Arrays.asList(tblProperties.getProperty(serdeConstants.LIST_COLUMNS).split(","));
        for (String col : fieldNames) {
            colNames.add(new Text(col));
        }

        colTypes = TypeInfoUtils.getTypeInfosFromTypeString(tblProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES));
        typeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(fieldNames, colTypes);
        inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
    }

    public static List<String> getFieldNames() {
        return fieldNames;
    }

    public static List<Text> getColNames() {
        return colNames;
    }

    public static StructTypeInfo getTypeInfo() {
        return typeInfo;
    }

    public static ObjectInspector getInspector() {
        return inspector;
    }

    public static List<TypeInfo> getColTypes() {
        return colTypes;
    }
}
