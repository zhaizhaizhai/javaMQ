package pku;


/**
 * 消费者
 * <p>
 * attachQueue
 * <p>
 * poll
 */

import java.io.IOException;
import java.util.*;


public class Consumer {

    private String queue;
    private List<String> bucketList = new ArrayList<>();
    private Set<String> buckets = new HashSet<>();

    private int lastIndex = 0;
    private final DemoMessageStore messageStore = new DemoMessageStore();


    public Consumer() {

    }

    public ByteMessage poll() {
        if (bucketList.size() == 0 || queue == null) {
            return null;
        }
        ByteMessage message;
        // // 慢轮询, 不致饿死后面的 topic, 又可提高 page cache 命中
        // 针对测试优化
        while (lastIndex < bucketList.size()) {

            message = messageStore.pollMessage(bucketList.get(lastIndex));
            if (message != null) {
                return message;
            }
            // 只有不命中时才 lastIndex++, 命中时(此 topic 有新 message)会下一次继续读此 topic
            lastIndex++;


        }
        return null;
    }


    public void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            return;
        }
        buckets.addAll(topics);
        bucketList.clear();
        bucketList.addAll(buckets);
        System.out.println(bucketList.size());
        // 排序, 提高 page cache 命中
        bucketList.sort(null);

        // 最后消费 queue
        queue = queueName;
        // bucketList.add(queueName);
    }


}