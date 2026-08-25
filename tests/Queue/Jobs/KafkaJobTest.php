<?php

namespace Rapide\LaravelQueueKafka\Tests\Queue\Jobs;

use Illuminate\Container\Container;
use Mockery;
use PHPUnit\Framework\TestCase;
use Rapide\LaravelQueueKafka\Queue\Jobs\KafkaJob;
use Rapide\LaravelQueueKafka\Queue\KafkaQueue;

class KafkaJobTest extends TestCase
{
    protected function makeJob(string $payload): KafkaJob
    {
        $message = Mockery::mock(\RdKafka\Message::class);
        $message->err = RD_KAFKA_RESP_ERR_NO_ERROR;
        $message->payload = $payload;
        $message->key = 'the-producer-key';

        return new KafkaJob(
            container: Mockery::mock(Container::class),
            connection: Mockery::mock(KafkaQueue::class),
            message: $message,
            connectionName: 'kafka',
            queue: 'default',
            topic: Mockery::mock(\RdKafka\ConsumerTopic::class),
        );
    }

    public function test_get_job_id_uses_payload_uuid_when_present(): void
    {
        $job = $this->makeJob('{"uuid":"job-uuid-123","payload":"payload"}');

        $this->assertSame('job-uuid-123', $job->getJobId());
    }

    public function test_get_job_id_falls_back_to_producer_key_when_uuid_missing(): void
    {
        $job = $this->makeJob('{"payload":"payload"}');

        $this->assertSame('the-producer-key', $job->getJobId());
    }

    protected function tearDown(): void
    {
        Mockery::close();
    }
}
