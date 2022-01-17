package kafka.examples.service;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * @author : wjiajun
 * @description:
 */
public class KafkaConsumerGroupService {

    private String brokerList;

    private AdminClient adminClient;

    private KafkaConsumer<String, String> kafkaConsumer;

    public KafkaConsumerGroupService(String brokerList) {
        this.brokerList = brokerList;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaConsumerGroupService kafkaConsumerGroupService = new KafkaConsumerGroupService("localhost:9092");
        kafkaConsumerGroupService.init();
        List<PartitionAssignmentState> kafkaAdminClientDemoGroupId = kafkaConsumerGroupService.collectGroupAssignment("kafkaAdminClientDemoGroupId");
        printPasList(kafkaAdminClientDemoGroupId);
    }

    public void init() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        adminClient = KafkaAdminClient.create(properties);
        kafkaConsumer = createKafkaConsumer(brokerList, "kafkaAdminClientDemoGroupId");
    }

    private KafkaConsumer<String, String> createKafkaConsumer(String brokerList, String groupId) {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaAdminClientDemoGroupId");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(properties);
    }

    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }

        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
    }

    public List<PartitionAssignmentState> collectGroupAssignment(String group) throws ExecutionException, InterruptedException {
        // 通过describeConsumerGroups请求获取当前消费组的元数据信息
        DescribeConsumerGroupsResult groupsResult = adminClient.describeConsumerGroups(Collections.singletonList(group));
        ConsumerGroupDescription description = groupsResult.all().get().get(group);
        List<TopicPartition> assignedTps = new ArrayList<>();
        List<PartitionAssignmentState> rowsWithConsumer = new ArrayList<>();
        Collection<MemberDescription> members = description.members();
        if (members != null) {
            ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(group);
            Map<TopicPartition, OffsetAndMetadata> offsets = offsetsResult.partitionsToOffsetAndMetadata().get();
            if (offsets != null && !offsets.isEmpty()) {
                String state = description.state().toString();
                if (Objects.equals(state, "Stable")) {
                    rowsWithConsumer = getRowsWithConsumer(description, offsets, members, assignedTps, group);
                }
            }

            List<PartitionAssignmentState> rowsWithoutConsumer = getRowWithoutConsumer(description, offsets, assignedTps, group);
            rowsWithConsumer.addAll(rowsWithoutConsumer);
        }
        return rowsWithConsumer;
    }

    private List<PartitionAssignmentState> getRowWithoutConsumer(ConsumerGroupDescription description, Map<TopicPartition, OffsetAndMetadata> offsets, List<TopicPartition> assignedTps, String group) {
        Set<TopicPartition> tpSet = offsets.keySet();
        return tpSet.stream()
                .filter(t -> !assignedTps.contains(t))
                .map(tp -> {
                    long logSize = 0;
                    Long endOffset = kafkaConsumer.endOffsets(Collections.singletonList(tp)).get(tp);
                    if (endOffset != null) {
                        logSize = endOffset;
                    }

                    long offset = offsets.get(tp).offset();
                    return new PartitionAssignmentState(group, description.coordinator(), tp.topic(), tp.partition(), logSize, getLag(offset, logSize), offset);
                }).sorted(Comparator.comparing(PartitionAssignmentState::getPartition))
                .collect(Collectors.toList());
    }

    /**
     * 有成员消费信息的处理
     */
    private List<PartitionAssignmentState> getRowsWithConsumer(ConsumerGroupDescription description, Map<TopicPartition, OffsetAndMetadata> offsets, Collection<MemberDescription> members, List<TopicPartition> assignedTps, String group) {
        List<PartitionAssignmentState> rowWithConsumer = new ArrayList<>();
        for (MemberDescription member : members) {
            MemberAssignment assignment = member.assignment();
            if (assignment == null) {
                continue;
            }

            Set<TopicPartition> tpSets = assignment.topicPartitions();
            if (tpSets.isEmpty()) {
                rowWithConsumer.add(new PartitionAssignmentState(group, description.coordinator(), member.consumerId(), member.host(), member.clientId()));
            } else {
                Map<TopicPartition, Long> logSizes = kafkaConsumer.endOffsets(tpSets);
                assignedTps.addAll(tpSets);
                List<PartitionAssignmentState> collect = tpSets.stream()
                        .sorted(Comparator.comparing(TopicPartition::partition))
                        .map(tp -> getPasWithConsumer(logSizes, offsets, tp, group, member, description))
                        .collect(Collectors.toList());
                rowWithConsumer.addAll(collect);
            }
        }
        return rowWithConsumer;
    }

    private PartitionAssignmentState getPasWithConsumer(Map<TopicPartition, Long> logSizes, Map<TopicPartition, OffsetAndMetadata> offsets, TopicPartition tp, String group, MemberDescription member, ConsumerGroupDescription description) {
        Long logSize = logSizes.get(tp);
        if (offsets.containsKey(tp)) {
            long offset = offsets.get(tp).offset();
            long lag = getLag(offset, logSize);
            return new PartitionAssignmentState(group, description.coordinator(), lag, tp.topic(), tp.partition(), offset, member.consumerId(), member.host(), member.clientId(), logSize);
        }
        return new PartitionAssignmentState(group, description.coordinator(), tp.topic(), tp.partition(), member.consumerId(), member.host(), member.clientId(), logSize);
    }

    private static long getLag(long offset, Long logSize) {
        long lag = logSize - offset;
        return lag < 0 ? 0 : lag;
    }

    private static void printPasList(List<PartitionAssignmentState> list) {
        System.out.println(String.format("%-40s %-10s %-15s %-15s %-10s %-50s%-30s %s", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID"));
        for (PartitionAssignmentState item : list) {
            System.out.println(String.format("%-40s %-10s %-15s %-15s %-10s %-50s%-30s %s",
                    item.topic, item.partition, item.offset, item.logSize, item.lag, item.consumerId, item.host, item.clientId));
        }

    }
}
