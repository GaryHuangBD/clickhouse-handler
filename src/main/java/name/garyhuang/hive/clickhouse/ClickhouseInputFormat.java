package name.garyhuang.hive.clickhouse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * @author GaryHuang
 * @date 2018/12/25
 **/
public class ClickhouseInputFormat implements InputFormat<NullWritable, Writable> {

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
        return new InputSplit[0];
    }

    @Override
    public RecordReader<NullWritable, Writable> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        throw new UnsupportedOperationException("Read from clickhouse is not supported");
    }
}
