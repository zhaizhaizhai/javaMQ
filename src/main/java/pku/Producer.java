package pku;


import java.io.IOException;

/**
 * 生产者
 * 更改的接口
 * <p>
 * send
 * <p>
 * flush
 */
public class Producer {


    private final DemoMessageStore messageStore = new DemoMessageStore();

    public Producer() {
    }


    //生成一个指定topic的message返回
    public ByteMessage createBytesMessageToTopic(String topic, byte[] body) {
        ByteMessage msg = new DefaultMessage(body);
        msg.putHeaders(MessageHeader.TOPIC, topic);
        return msg;
    }


    //将message发送出去
    public void send(ByteMessage message) {
        String topic = message.headers().getString(MessageHeader.TOPIC);
        messageStore.putMessage(topic, message);

    }


    //处理将缓存区的剩余部分
    public void flush() throws Exception {

    }


}
