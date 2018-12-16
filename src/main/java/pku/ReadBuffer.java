package pku;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


/**
 * READ ONLY MappedByteBuffer Wrapper for Consumer
 */
// 仅用作 Consumer 的私有属性, 且对文件只读, 不会有竞争
public class ReadBuffer {

    private final int bufferSize = Constants.LOG_BUFFER_SIZE;

    private Topic topic = null;
    private FileChannel fileChannel;
    private MappedByteBuffer buffer;
    private long offsetInFile; // 映射区的末尾在源文件中的 offset


    public ReadBuffer() {
    }

    /**
     * return false when no more file content to map.
     */
    public boolean reMap(Topic topic) {
        fileChannel = topic.getLogFile().getFileChannel();
        try {
            buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, bufferSize);
            offsetInFile = bufferSize;
            this.topic = topic;
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean reMap() {
        try {
            if (fileChannel.size() - offsetInFile >= bufferSize) {
                buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, offsetInFile, bufferSize);
                offsetInFile += bufferSize;
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }


    public ByteMessage read(Topic topic) {
        // readIndexFileBuffer 缓存不命中
        if (this.topic != topic) {
            // 1. 不是同一个 topic
            if (!reMap(topic))
                return null; // no more to map == no more new record
        }
        int length;
        byte[] bytes;
        length = readInt(); // byteBody.lengt
        if (length == 0)
            return null;
        bytes = readByte(length); // byteBody
        DefaultMessage message = new DefaultMessage(bytes);
        length = readInt(); // byteHeaders.length
        bytes = readByte(length); // byteBody
        bytesToDefaultKeyValue((DefaultKeyValue) (message.headers()), bytes, 0, length); // byteHeaders
        return message;

    }


    public byte[] readByte(int length) {
        byte[] result = new byte[length];
        if (buffer.remaining() < length) { // 读两段
            int size1 = buffer.remaining();
            buffer.get(result, 0, size1);
            if (!reMap())
                return null; // ERROR 文件损坏？
            buffer.get(result, size1, length - size1);
        } else { // other, 正常读
            buffer.get(result);
        }
        return result;
    }

    public int readInt() {
        if (buffer.remaining() < 4) { // 读两段
            byte[] result = new byte[4];
            int size1 = buffer.remaining();
            buffer.get(result, 0, size1);
            if (!reMap())
                return 0; // ERROR 文件损坏？
            buffer.get(result, size1, 4 - size1);
            return Utils.getInt(result, 0);
        } else { // other, 正常读
            return buffer.getInt();
        }

    }


    public DefaultKeyValue bytesToDefaultKeyValue(DefaultKeyValue kv, byte[] kvBytes, int offset, int length) {
        int end = offset + length;
        // short for keys
        //short shortValue;
        int intValue;
        long longValue;
        double doubleValue;
        String key, stringValue;
        while (offset < end) {
            //shortValue = Utils.getShort(kvBytes,offset);
            intValue = Utils.getInt(kvBytes, offset);
            offset += 2;
            key = new String(kvBytes, offset, intValue);
            offset += intValue;
            switch (kvBytes[offset++]) {
                case 0: // for int
                    intValue = Utils.getInt(kvBytes, offset);
                    offset += 4;
                    kv.put(key, intValue);
                    break;
                case 1: // for long
                    longValue = Utils.getLong(kvBytes, offset);
                    offset += 8;
                    kv.put(key, longValue);
                    break;
                case 2: // for double
                    doubleValue = Utils.getDouble(kvBytes, offset);
                    offset += 8;
                    kv.put(key, doubleValue);
                    break;
                case 3: // for string
                    intValue = Utils.getInt(kvBytes, offset);
                    offset += 4;
                    stringValue = new String(kvBytes, offset, intValue);
                    offset += intValue;
                    kv.put(key, stringValue);
                    break;
                default:
                    System.err.println("ERROR: bytesToDefaultKeyValue");
                    break;
            }
        }
        return kv;
    }
}
