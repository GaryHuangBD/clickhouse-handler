package name.garyhuang.hive.clickhouse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author GaryHuang
 * @date 2018/12/26
 **/
public class ClickhouseSerde extends AbstractSerDe {

    private ClickhouseWritable writable;

    @Override
    public void initialize(@Nullable Configuration configuration, Properties tblProperties) throws SerDeException {
        HiveTableSchema.init(tblProperties);
        writable = new ClickhouseWritable();
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return ClickhouseWritable.class;
    }

    @Override
    public Writable serialize(Object data, ObjectInspector objInspector) throws SerDeException {
        // Make sure we have a struct, as Hive "root" fields should be a struct
        if (objInspector.getCategory() != ObjectInspector.Category.STRUCT) {
            throw new SerDeException("Unable to serialize root type of " + objInspector.getTypeName());
        }

        writable.clear();
        // Fields
        writable.init(data, objInspector);

        return writable;
    }

    @Override
    public SerDeStats getSerDeStats() {
        // no support for statistics
        return null;
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        throw new UnsupportedOperationException("Read from ClickHouse is not supported");
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return HiveTableSchema.getInspector();
    }
}
