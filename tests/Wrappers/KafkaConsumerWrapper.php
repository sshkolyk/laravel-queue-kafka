<?php

namespace Rapide\LaravelQueueKafka\Tests\Wrappers;

use RdKafka\KafkaConsumer;

class KafkaConsumerWrapper
{
    public function __construct(private \RdKafka\KafkaConsumer $consumer) {}

    public function queryWatermarkOffsets($topic, $partition = 0, &$low = 0, &$high = 0, $timeout = 0): void
    {
        $this->consumer->queryWatermarkOffsets($topic, $partition, $low, $high, $timeout);
    }

    public function getCommittedOffsets($topicPartitions, $timeout = 0): array
    {
        return $this->consumer->getCommittedOffsets($topicPartitions, $timeout);
    }
}
