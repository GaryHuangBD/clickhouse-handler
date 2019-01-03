package name.garyhuang.hive.clickhouse.jdbc;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import name.garyhuang.hive.clickhouse.ClickhouseWritable;
import name.garyhuang.hive.clickhouse.HiveTableSchema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import parquet.org.slf4j.Logger;
import parquet.org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static name.garyhuang.hive.clickhouse.ClickhouseConstants.*;

/**
 * @author GaryHuang
 * @date 2018/12/25
 **/
public class JdbcWriter implements FileSinkOperator.RecordWriter, RecordWriter<BytesWritable, ClickhouseWritable> {

    private final static String sep = ",";

    private final Integer batchSize;
    private final Progressable progressable;
    private final String taskId;

    private final String table;

    private final boolean isIgnoreError;

    private ClickHouseRowBuffer buffer;

    private Random random;

    private PreparedStatement pstmt;

    public JdbcWriter(JobConf jobConf, String taskId, Progressable progressable) {
        this.progressable = progressable;
        this.taskId = taskId;
        this.batchSize = Integer.parseInt(jobConf.get(CLICKHOUSE_WRITE_SIZE, DEFAULT_CLICKHOUSE_WRITE_SIZE));
        this.table = jobConf.get(CLICKHOUSE_TABLE);
        this.isIgnoreError = Boolean.parseBoolean(jobConf.get(CLICKHOUSE_IGNORE_ERROR, DEFAULT_CLICKHOUSE_IGNORE_ERROR));
        long flushTimeout = Long.parseLong(jobConf.get(CLICKHOUSE_FLUSH_TIMEOUT, DEFAULT_CLICKHOUSE_FLUSH_TIMEOUT));
        buffer = new ClickHouseRowBuffer(batchSize, flushTimeout);

        String servers = jobConf.get(CLICKHOUSE_SERVERS);
        String db = jobConf.get(CLICKHOUSE_DB);
        String user = jobConf.get(CLICKHOUSE_USER);
        String password = jobConf.get(CLICKHOUSE_PASSWORD);


        String[] serverArray = servers.split(sep);
        random = new Random();
        try {
            String sql = genInitSQL(table);
            System.out.println("sql: " + sql);
            String server = chooseServer(taskId, serverArray);
            System.out.println("init clickhouse db connection, server: " + server);
            Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
            String serverDB = StringUtils.remove(server, ' ') + "/" + db;
            Connection connection = DriverManager.getConnection("jdbc:clickhouse://" + serverDB, user, password);
            pstmt = connection.prepareStatement(sql);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private String chooseServer(String taskId, String[] serverArray) {
        String taskLastId = taskId.substring(taskId.lastIndexOf("_") + 1);
        Integer id = Ints.tryParse(taskLastId);
        int idx = (id == null ? random.nextInt() : id) % serverArray.length;
        return serverArray[idx];
    }

    private String genInitSQL(String table) {
        StringBuffer sb = new StringBuffer("insert into ").append(table)
                .append(" (");
        List<String> names = HiveTableSchema.getFieldNames();
        String nameStr = StringUtils.join(names, ',');
        sb.append(nameStr).append(")  values (");
        for (int i =0; i < names.size(); i++) {
            sb.append(" ?,");
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append(")");
        return sb.toString();
    }


    @Override
    public void write(Writable w) throws IOException {
        ClickhouseWritable clickhouseWritable = (ClickhouseWritable) w;
        Map<String, Object> map = Maps.newHashMapWithExpectedSize(clickhouseWritable.size());
        for (Map.Entry<Writable, Writable> entry : clickhouseWritable.entrySet()) {
            String key = entry.getKey().toString();
            Writable valueObject = entry.getValue();
            Object value = clickhouseWritable.getValue(key, valueObject);
            map.put(key, value);
        }
        buffer.add(map);

        try {
            if (buffer.shouldFlush()) {
                send();
            }
        } catch (SQLException e) {
            System.out.println("Error when sending data to clickhouse" + e.getMessage());
            if (!isIgnoreError) {
                throw new IOException(e);
            }
        }
    }

    public void send() throws SQLException {
        System.out.println("write to clickhouse");
        List<String> fieldNames = HiveTableSchema.getFieldNames();
        pstmt.clearBatch();
        for (int i = 0; i < buffer.currentSize(); i++) {
            Map<String, Object> kvMap = buffer.get(i);
            for (int j = 0; j < fieldNames.size(); j++) {
                String name = fieldNames.get(j);
                pstmt.setObject(j + 1, kvMap.get(name));
            }
            pstmt.addBatch();
        }
        pstmt.executeBatch();
        buffer.clear();
    }

    @Override
    public void close(boolean abort) throws IOException {
        try {
            if (buffer.currentSize() > 0) {
                send();
            }
            pstmt.getConnection().close();
        } catch (SQLException e) {
            System.out.println("Clickhouse connect close error: " + e.getMessage());
            e.printStackTrace();
        }
    }


    @Override
    public void write(BytesWritable bytesWritable, ClickhouseWritable clickhouseWritable) throws IOException {
        write(clickhouseWritable);
    }

    @Override
    public void close(Reporter reporter) throws IOException {

    }
}
