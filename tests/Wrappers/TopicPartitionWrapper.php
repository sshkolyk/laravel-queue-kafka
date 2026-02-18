<?php

namespace Rapide\LaravelQueueKafka\Tests\Wrappers;

use RdKafka\TopicPartition;

class TopicPartitionWrapper
{
    private int $offset = -1001;

    public function __construct(
        private string $topic,
        private int $partition,
    ) {}

    public function getTopic(): string
    {
        return $this->topic;
    }

    public function getPartition(): int
    {
        return $this->partition;
    }

    public function getOffset(): int
    {
        return $this->offset;
    }

    public function setOffset($offset): void
    {
        $this->offset = $offset;
    }

    public function toNative(): \RdKafka\TopicPartition
    {
        $tp = new \RdKafka\TopicPartition($this->topic, $this->partition);
        $tp->setOffset($this->offset);
        return $tp;
    }
}
