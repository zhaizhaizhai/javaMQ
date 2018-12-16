package pku;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;


// 一个 Producer/Consumer 一个，不会有并发
public class DemoMessageStore {

    private HashMap<String, Topic> topicCache = new HashMap<>();


    // for Producer
    private final ByteBuffer KVToBytesBuffer = ByteBuffer.allocate(256 * 1024);

    // for Consumer
    // 存 <bucket name, offsetInIndexFile>
    private final ReadBuffer readBuffer = new ReadBuffer();

    public DemoMessageStore() {
    }

    // for Produce
    public void putMessage(String bucket, ByteMessage message) {
        Topic topic;
        if ((topic = topicCache.get(bucket)) == null) {
            topic = GlobalResource.getTopicByName(bucket);
            topicCache.put(bucket, topic);
        }
        byte[] messageByte = messageToBytes(message);

        try {
            topic.getWriteBuffer().write(messageByte);

        } catch (
                Exception e) {
            e.printStackTrace();
        }

    }

    // for Consumer, 利用自己的 readIndexFileBuffer, readLogFileBuffer 快速消费
    public ByteMessage pollMessage(String bucket) {
        Topic topic;
        if ((topic = topicCache.get(bucket)) == null) {
            topic = GlobalResource.getTopicByName(bucket);
            topicCache.put(bucket, topic);
        }
        return readBuffer.read(topic);
    }

    // for Producer

    /**
     * message 结构
     * -------------------------------------------
     * |body.length| body |headers.length|headers|
     * |____int____|byte[]|_____int______|byte[] |
     * -------------------------------------------
     */
    public byte[] messageToBytes(ByteMessage message) {
        byte[] byteHeaders = defaultKeyValueToBytes((DefaultKeyValue) (message.headers()));
        byte[] byteBody = ((DefaultMessage) message).getBody();
        byte[] result = new byte[2 * 4 + byteBody.length + byteHeaders.length];
        int pos = 0;
        Utils.intToByteArray(byteBody.length, result, pos); // byteBody.length
        pos += 4;
        System.arraycopy(byteBody, 0, result, pos, byteBody.length); // byteBody
        pos += byteBody.length;
        Utils.intToByteArray(byteHeaders.length, result, pos); // byteHeaders.length
        pos += 4;
        System.arraycopy(byteHeaders, 0, result, pos, byteHeaders.length); // byteHeaders
        return result;
    }


    // for Producer
    public byte[] defaultKeyValueToBytes(DefaultKeyValue kv) {
        if (kv == null) {
            return new byte[0];
        }
        String key;
        Object value;
        byte[] keyBytes, stringValueBytes;
        KVToBytesBuffer.clear();
        Iterator<Map.Entry<String, Object>> iterator = kv.getKVS().entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, Object> entry = iterator.next();
            // key
            key = entry.getKey();
            keyBytes = key.getBytes();
            KVToBytesBuffer.putInt( keyBytes.length);
            KVToBytesBuffer.put(keyBytes);
            // value
            value = entry.getValue();
            if (value instanceof Integer) {
                KVToBytesBuffer.put((byte) 0);
                KVToBytesBuffer.putInt((Integer) value);
            } else if (value instanceof Long) {
                KVToBytesBuffer.put((byte) 1);
                KVToBytesBuffer.putLong((Long) value);
            } else if (value instanceof Double) {
                KVToBytesBuffer.put((byte) 2);
                KVToBytesBuffer.putDouble((Double) value);
            } else {
                KVToBytesBuffer.put((byte) 3);
                stringValueBytes = ((String) value).getBytes();
                KVToBytesBuffer.putInt(stringValueBytes.length);
                KVToBytesBuffer.put(stringValueBytes);
            }
        }
        KVToBytesBuffer.flip();
        byte[] result = new byte[KVToBytesBuffer.remaining()];
        KVToBytesBuffer.get(result);
        return result;
    }


    // for Producer
    public void flush() {

    }

    public static void main(String[] args) {
        ByteMessage message = new DefaultMessage();
        message.putHeaders(MessageHeader.TOPIC, "topic0");
        message.putHeaders(MessageHeader.SEARCH_KEY, "hello");
        byte[] body = {0, 1, 2, 3};
        message.setBody(body);
//
//        byte[] b = new DemoMessageStore().messageToBytes(message);


        new DemoMessageStore().putMessage("topic10", message);





        /*
        *
     0 9 83 101 97 114 99 104 75 101 121 3 0 0 0 5 104 101 108 108 111 0 5 84 111 112 105 99 3 0 0 0 6 116 111 112 105 99 48

         *
        *
        * */

    }

}



