package name.garyhuang.hive.clickhouse;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.List;

/**
 * @author GaryHuang
 * @date 2018/12/26
 **/
public class ClickhouseWritable extends MapWritable {

    public void init(Object data, ObjectInspector objInspector) {
        StructObjectInspector structInspector = (StructObjectInspector) objInspector;
        List<? extends StructField> fields = structInspector.getAllStructFieldRefs();
        for (int i = 0; i < fields.size(); i++) {
            StructField structField = fields.get(i);

            ObjectInspector foi = structField.getFieldObjectInspector();
            ObjectInspector.Category foiCategory = foi.getCategory();
            Object structFieldData = structInspector.getStructFieldData(data, structField);
            if (structFieldData == null) {
                continue;
            }

            if (foiCategory.equals(ObjectInspector.Category.PRIMITIVE)) {
                PrimitiveObjectInspector loi = (PrimitiveObjectInspector)foi;
                Writable valueWritable = (Writable)loi.getPrimitiveWritableObject(loi.copyObject(structFieldData));
                Text fieldName = HiveTableSchema.getColNames().get(i);
                put(fieldName, valueWritable);
            } else {
                // TODO to support other field type
                continue;
            }
        }
    }

    public Object getValue(String key, Writable value) {
        PrimitiveTypeInfo fieldTypeInfo = (PrimitiveTypeInfo)HiveTableSchema.getTypeInfo().getStructFieldTypeInfo(key);
        return PrimitiveObjectInspectorFactory
                .getPrimitiveWritableObjectInspector(fieldTypeInfo)
                .getPrimitiveJavaObject(value);
    }
}
