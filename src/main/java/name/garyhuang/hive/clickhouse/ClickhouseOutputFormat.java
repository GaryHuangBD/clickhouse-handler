package name.garyhuang.hive.clickhouse;

import name.garyhuang.hive.clickhouse.jdbc.JdbcWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

/**
 * @author GaryHuang
 * @date 2018/12/25
 **/
public class ClickhouseOutputFormat implements HiveOutputFormat<NullWritable, Writable> {
    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc,
                                                             Path finalOutPath, Class<? extends Writable> valueClass,
                                                             boolean isCompressed, Properties tableProperties,
                                                             Progressable progress) throws IOException {

        return new JdbcWriter(jc, Utilities.getTaskId(jc), progress);
    }

    @Override
    public RecordWriter<NullWritable, Writable> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String name, Progressable progressable) throws IOException {
        throw new RuntimeException("this is not suppose to be here");
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {

    }
}
