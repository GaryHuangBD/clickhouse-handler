package name.garyhuang.hive.clickhouse;

import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

import java.util.Map;
import java.util.Properties;

import static name.garyhuang.hive.clickhouse.ClickhouseConstants.*;

/**
 * @author GaryHuang
 * @date 2018/12/25
 **/
public class ClickhouseStorageHandler extends DefaultStorageHandler {

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return ClickhouseInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return ClickhouseOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return ClickhouseSerde.class;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        setProperties(tableDesc.getProperties(), jobProperties);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        setProperties(tableDesc.getProperties(), jobProperties);
    }

    private void setProperties(Properties tableProps, Map<String, String> jobProps) {
        String servers = tableProps.getProperty(CLICKHOUSE_SERVERS);
        if (servers != null) {
            jobProps.put(CLICKHOUSE_SERVERS, servers);
        }

        String db = tableProps.getProperty(CLICKHOUSE_DB);
        if (db != null) {
            jobProps.put(CLICKHOUSE_DB, db);
        }

        String table = tableProps.getProperty(CLICKHOUSE_TABLE);
        if (table != null) {
            jobProps.put(CLICKHOUSE_TABLE, table);
        }

        String pass = tableProps.getProperty(CLICKHOUSE_PASSWORD);
        if (pass != null) {
            jobProps.put(CLICKHOUSE_PASSWORD, pass);
        }

        String user = tableProps.getProperty(CLICKHOUSE_USER);
        if (user != null) {
            jobProps.put(CLICKHOUSE_USER, user);
        }

        String size = tableProps.getProperty(CLICKHOUSE_WRITE_SIZE);
        if (size != null) {
            jobProps.put(CLICKHOUSE_WRITE_SIZE, size);
        }

        String flushTimeout = tableProps.getProperty(CLICKHOUSE_FLUSH_TIMEOUT);
        if (size != null) {
            jobProps.put(CLICKHOUSE_FLUSH_TIMEOUT, flushTimeout);
        }

        String ignoreError = tableProps.getProperty(CLICKHOUSE_IGNORE_ERROR);
        if (size != null) {
            jobProps.put(CLICKHOUSE_IGNORE_ERROR, ignoreError);
        }
    }


}
