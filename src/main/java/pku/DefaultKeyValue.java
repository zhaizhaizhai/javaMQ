package pku;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 一个Key-Value的实现
 */
public class DefaultKeyValue implements KeyValue {
    private int length = 0;
    private final HashMap<String, Object> kvs = new HashMap<>();

    public Object getObj(String key) {
        return kvs.get(key);
    }

    public HashMap<String, Object> getMap() {
        return kvs;
    }


    public HashMap<String, Object> getKVS() {
        return kvs;
    }


    public int getInt(String key) {
        return (Integer) kvs.getOrDefault(key, 0);
    }

    public long getLong(String key) {
        return (Long) kvs.getOrDefault(key, 0L);
    }

    public double getDouble(String key) {
        return (Double) kvs.getOrDefault(key, 0.0d);
    }

    public String getString(String key) {
        return (String) kvs.getOrDefault(key, null);
    }

    public Set<String> keySet() {
        return kvs.keySet();
    }

    public boolean containsKey(String key) {
        return kvs.containsKey(key);
    }


    public KeyValue put(String key, int value) {
        String value2 = String.valueOf(value);
        length += key.getBytes().length + value2.getBytes().length;
        kvs.put(key, value2);
        return this;
    }

    @Override
    public KeyValue put(String key, long value) {
        String value2 = String.valueOf(value);
        length += key.getBytes().length + value2.getBytes().length;
        kvs.put(key, value2);
        return this;
    }

    @Override
    public KeyValue put(String key, double value) {
        String value2 = String.valueOf(value);
        length += key.getBytes().length + value2.getBytes().length;
        kvs.put(key, value2);
        return this;
    }

    @Override
    public KeyValue put(String key, String value) {
        length += key.getBytes().length + value.getBytes().length;
        kvs.put(key, value);
        return this;
    }

    /**
     * 用于不知道key对应于什么类型的情况
     *
     * @param key
     * @return
     */
    public Object get(final String key) {
        return kvs.getOrDefault(key, null);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (Map.Entry<String, Object> entry : kvs.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            sb.append(key).append(':').append(value.toString()).append(", ");

        }
        sb.append(']');
        return sb.toString();
    }

    public int getLength() {
        return length;
    }
}
