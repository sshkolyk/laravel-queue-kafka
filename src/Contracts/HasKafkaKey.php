<?php

namespace Rapide\LaravelQueueKafka\Contracts;

interface HasKafkaKey
{
    /**
     * The Kafka producer key used to route this job to a partition.
     * Jobs sharing the same key are routed to the same partition,
     * preserving their processing order.
     */
    public function kafkaKey(): string;
}
