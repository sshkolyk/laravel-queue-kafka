<?php

namespace Rapide\LaravelQueueKafka\Tests\Jobs;

use Rapide\LaravelQueueKafka\Contracts\HasKafkaKey;

class TestJobWithKafkaKey implements HasKafkaKey
{
    public function __construct(private readonly string $key) {}

    public function handle(): void {}

    public function kafkaKey(): string
    {
        return $this->key;
    }
}
