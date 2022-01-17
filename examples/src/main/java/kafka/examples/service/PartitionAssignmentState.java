package kafka.examples.service;

import org.apache.kafka.common.Node;

/**
 * @author : wjiajun
 * @description:
 */
public class PartitionAssignmentState {

    public String group;

    public Node coordinator;

    public String topic;

    public int partition;

    public long offset;

    public long lag;

    public String consumerId;

    public String host;

    public String clientId;

    public long logSize;

    public PartitionAssignmentState(String group, Node coordinator, String consumerId, String host,  String clientId) {
        this.group = group;
        this.coordinator = coordinator;
        this.consumerId = consumerId;
        this.host = host;
        this.clientId = clientId;
    }

    public PartitionAssignmentState(String group, Node coordinator, long lag, String topic, int partition, long offset, String consumerId, String host, String clientId, long logSize) {
        this.group = group;
        this.coordinator = coordinator;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.lag = lag;
        this.consumerId = consumerId;
        this.host = host;
        this.clientId = clientId;
        this.logSize = logSize;
    }

    public PartitionAssignmentState(String group, Node coordinator, String topic, int partition, String consumerId, String host, String clientId, long logSize) {
        this.group = group;
        this.coordinator = coordinator;
        this.topic = topic;
        this.partition = partition;
        this.consumerId = consumerId;
        this.host = host;
        this.clientId = clientId;
        this.logSize = logSize;
    }

    public PartitionAssignmentState(String group, Node coordinator, String topic, int partition, long logSize, long lag, long offset) {
        this.group = group;
        this.coordinator = coordinator;
        this.topic = topic;
        this.partition = partition;
        this.logSize = logSize;
        this.lag = lag;
        this.offset = offset;
    }

    public int getPartition() {
        return this.partition;
    }
}
