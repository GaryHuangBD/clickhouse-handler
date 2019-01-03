package name.garyhuang.hive.clickhouse.jdbc;


import com.google.common.collect.Lists;
import name.garyhuang.hive.clickhouse.ClickhouseWritable;
import name.garyhuang.hive.clickhouse.HiveTableSchema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static name.garyhuang.hive.clickhouse.ClickhouseConstants.*;

public class JdbcWriterTest {

    private final static int RECORD_NUMBER = 10;

    private List<String> columnNames = Lists.newArrayList("along","adouble","astring");

    private List<String> columnTypes = Lists.newArrayList("bigint", "double", "string");

    private List<ClickhouseWritable> RECORDS;

    private JdbcWriter writer;

    private Properties properties;

    @Before
    public void setUp() throws Exception {
        JobConf conf = new JobConf();
        conf.set(CLICKHOUSE_SERVERS, "127.0.0.1:9000");
        conf.set(CLICKHOUSE_DB, "default");
        conf.set(CLICKHOUSE_TABLE, "test1");
        conf.set(CLICKHOUSE_USER, "default");
        conf.set(CLICKHOUSE_PASSWORD, "IARYxRcr");
        conf.set(CLICKHOUSE_WRITE_SIZE, "2");

        HiveTableSchema.init(createProperties());
        writer = new JdbcWriter(conf, "11", null);

        List<Text> columnTextNames = columnNames.stream().map(c -> new Text(c)).collect(Collectors.toList());
        RECORDS = IntStream
                .range(0, RECORD_NUMBER)
                .mapToObj(number -> {
                    ClickhouseWritable writable = new ClickhouseWritable();
                    String value = String.valueOf(number);
                    writable.put(
                            columnTextNames.get(0),
                            (Writable)PrimitiveObjectInspectorFactory.javaLongObjectInspector
                                    .getPrimitiveWritableObject(Long.parseLong(value))
                    );
                    writable.put(
                            columnTextNames.get(1),
                            (Writable)PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
                                    .getPrimitiveWritableObject(Double.parseDouble(value))
                    );
                    writable.put(
                            columnTextNames.get(2),
                            PrimitiveObjectInspectorFactory.javaStringObjectInspector
                                    .getPrimitiveWritableObject(value)
                    );
                    return writable;
                }).collect(Collectors.toList());
//        ClickhouseWritable writable = new ClickhouseWritable();
//        writable.put(
//                columnTextNames.get(2),
//                PrimitiveObjectInspectorFactory.javaStringObjectInspector
//                        .getPrimitiveWritableObject("test")
//        );
//        RECORDS.add(writable);
    }


    @Test
    public void testWrite() throws Exception {
        RECORDS.stream().forEach(writable -> {
            try {
                writer.write(writable);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        try {
            writer.close(false);
        } catch (IOException e) {
        }
    }


    private Properties createProperties() {

        Properties tbl = new Properties();
        // Set the configuration parameters
        tbl.setProperty(serdeConstants.SERIALIZATION_FORMAT, "9");
        tbl.setProperty("columns",
                StringUtils.join(columnNames, ','));
        tbl.setProperty("columns.types",
                StringUtils.join(columnTypes, ':'));
        tbl.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");
        return tbl;
    }

}