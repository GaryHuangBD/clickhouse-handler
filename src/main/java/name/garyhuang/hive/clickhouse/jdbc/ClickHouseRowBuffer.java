package name.garyhuang.hive.clickhouse.jdbc;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * @author GaryHuang
 * @date 2018/12/27
 **/
public class ClickHouseRowBuffer {

    private final int flushSize;

    private final long bufferTimeoutMS;

    private List<Map<String, Object>> rows;

    private long lastTime;

    public ClickHouseRowBuffer(int size, long bufferTimeoutMS) {
        this.lastTime = System.currentTimeMillis();
        this.flushSize = size;
        this.bufferTimeoutMS = bufferTimeoutMS;
        this.rows = Lists.newArrayListWithCapacity(size);
    }

    public void add(Map<String, Object> map) {
        rows.add(map);
    }

    public int currentSize() {
        return rows.size();
    }

    public Map<String, Object> get(int idx) {
        return rows.get(idx);
    }

    public void clear() {
        rows.clear();
        lastTime = System.currentTimeMillis();
    }

    public boolean shouldFlush() {
        long now = System.currentTimeMillis();
        return rows.size() >= flushSize || lastTime + bufferTimeoutMS >= now;
    }

    public String genInertSql(String table) {
        StringBuffer sb = new StringBuffer();
        Map<String, Object> map;
        for (int i = 0; i < rows.size(); i ++) {
            map = rows.get(i);
            String names = StringUtils.join(map.keySet(), ',');
            sb.append("insert into ").append(table).append( " (")
                    .append(names)
                    .append(") values (");
            for (Object value : map.values()) {
                if (value instanceof Number) {
                    sb.append(value).append(",");
                } else {
                    sb.append("'").append(value).append("',");
                }
            }
            sb.deleteCharAt(sb.length() -1);
            sb.append(");");
        }
        return sb.toString();

    }




}
