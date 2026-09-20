<?php

namespace Rapide\LaravelQueueKafka\Tests;

use Illuminate\Container\Container;
use Illuminate\Support\Facades\Facade;
use Illuminate\Support\Facades\Log;
use Mockery;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Rapide\LaravelQueueKafka\Exceptions\QueueKafkaException;
use Rapide\LaravelQueueKafka\Queue\Jobs\KafkaJob;
use Rapide\LaravelQueueKafka\Queue\KafkaQueue;
use Rapide\LaravelQueueKafka\Tests\Jobs\TestJob;
use Rapide\LaravelQueueKafka\Tests\Jobs\TestJobWithKafkaKey;
use Rapide\LaravelQueueKafka\Tests\Wrappers\KafkaConfWrapper;
use Rapide\LaravelQueueKafka\Tests\Wrappers\KafkaConsumerWrapper;
use Rapide\LaravelQueueKafka\Tests\Wrappers\TopicPartitionWrapper;
use ReflectionMethod;
use ReflectionProperty;

class KafkaQueueTest extends TestCase
{
    protected Container $container;

    protected array $config;

    protected ?KafkaQueue $queue;

    protected ?\RdKafka\Producer $producer;

    protected ?\RdKafka\Consumer $consumer;

    protected ?\RdKafka\ConsumerTopic $consumerTopicMock;

    protected function setUp(): void
    {
        parent::setUp();

        $this->producer = Mockery::mock(\RdKafka\Producer::class);
        $this->consumer = Mockery::mock(\RdKafka\Consumer::class);
        $this->container = Mockery::mock(\Illuminate\Container\Container::class);
        $this->consumerTopicMock = Mockery::mock(\RdKafka\ConsumerTopic::class);
        $this->consumerTopicMock->shouldReceive('produce')->andReturnNull();
        $this->consumerTopicMock->shouldReceive('consumeStart')->andReturnNull();
        $this->consumerTopicMock->shouldReceive('consumeStop')->andReturnNull();
        $this->consumerTopicMock->shouldReceive('consume')->andReturn(
            Mockery::mock(
                \RdKafka\Message::class,
                static function (\RdKafka\Message $m) {
                    $m->err = RD_KAFKA_RESP_ERR_NO_ERROR;
                }
            )
        );

        $this->consumer->shouldReceive('newTopic')
            ->with(Mockery::any())
            ->andReturn($this->consumerTopicMock);

        Log::spy();

        $this->config = [
            'queue' => \Illuminate\Support\Str::random(),
            'sleep_on_error' => 2,
            'sasl_enable' => false,
            'brokers' => 'localhost:9092',
            'producer_partitioner' => 'murmur2_random',
            'consumer_partition' => 0,
            'timeout_ms' => 2000,
            'auto_offset_reset' => 'earliest',
            'consumer_group_id' => 'laravel_queue',
            'auto_commit' => true,
            'auto_commit_interval_ms' => 5000,
            'stop_consume_on_empty' => false,
        ];

        $this->queue = new KafkaQueue($this->config);
        $refProducer = new \ReflectionProperty($this->queue, '_producer');
        $refProducer->setValue($this->queue, $this->producer);
        $refConsumer = new \ReflectionProperty($this->queue, '_consumer');
        $refConsumer->setValue($this->queue, $this->consumer);
        $this->queue->setContainer($this->container);
    }

    public function test_get_consumer(): void
    {
        $getConsumer = new ReflectionMethod($this->queue, 'getConsumer');
        $consumer = $getConsumer->invoke($this->queue);

        $this->assertEquals($consumer, $this->consumer);
    }

    public function test_set_config_rebuilds_cached_kafka_clients_when_configuration_changes(): void
    {
        $consumerConfig = new ReflectionProperty($this->queue, '_consumer_conf');
        $consumerConfig->setValue($this->queue, new \RdKafka\Conf);

        $consumerTopics = new ReflectionProperty($this->queue, '_consumer_topics');
        $consumerTopics->setValue($this->queue, ['orders' => $this->consumerTopicMock]);

        $this->queue->setConfig(array_merge($this->config, ['consumer_partition' => 1]));

        $producer = new ReflectionProperty($this->queue, '_producer');
        $consumer = new ReflectionProperty($this->queue, '_consumer');

        $this->assertNull($producer->getValue($this->queue));
        $this->assertNull($consumerConfig->getValue($this->queue));
        $this->assertNull($consumer->getValue($this->queue));
        $this->assertSame([], $consumerTopics->getValue($this->queue));
    }

    #[DataProvider('autoCommitValues')]
    public function test_auto_commit_is_applied(bool $autoCommit, string $expected): void
    {
        $conf = new KafkaConfWrapper;
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.conf', Mockery::any())
            ->andReturn($conf);

        $this->queue->setConfig(array_merge($this->config, ['auto_commit' => $autoCommit]));

        $getConsumerConfig = new ReflectionMethod($this->queue, 'getConsumerConfig');
        $getConsumerConfig->invoke($this->queue);

        $this->assertSame($expected, $conf->settings['enable.auto.commit']);
    }

    public static function autoCommitValues(): array
    {
        return [
            'bool true' => [true, 'true'],
            'bool false' => [false, 'false'],
        ];
    }

    public function test_auto_commit_interval_is_applied_to_the_low_level_consumer(): void
    {
        $conf = new KafkaConfWrapper;
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.conf', Mockery::any())
            ->andReturn($conf);

        $this->queue->setConfig(array_merge($this->config, ['auto_commit_interval_ms' => 10]));

        $getConsumerConfig = new ReflectionMethod($this->queue, 'getConsumerConfig');
        $getConsumerConfig->invoke($this->queue);

        $this->assertSame('10', $conf->settings['topic.auto.commit.interval.ms']);
    }

    public function test_size_when_internal_exception(): void
    {
        $size = $this->queue->size();
        $this->assertEquals(0, $size);
    }

    public function test_size_normal(): void
    {
        $this->mockQueueOffsets(low: 11, high: 115, committed: 101);

        $this->assertSame(14, $this->queue->size());
    }

    public function test_size_without_committed_offset_uses_available_watermark_range(): void
    {
        $this->mockQueueOffsets(low: 11, high: 115, committed: -1001);

        $this->assertSame(104, $this->queue->size());
    }

    public function test_size_is_zero_for_empty_partition_without_committed_offset(): void
    {
        $this->mockQueueOffsets(low: 115, high: 115, committed: -1001);

        $this->assertSame(0, $this->queue->size());
    }

    public function test_size_is_zero_when_committed_offset_reaches_high_watermark(): void
    {
        $this->mockQueueOffsets(low: 11, high: 115, committed: 115);

        $this->assertSame(0, $this->queue->size());
    }

    public function test_size_is_zero_for_negative_special_offset(): void
    {
        $this->mockQueueOffsets(low: 11, high: 115, committed: RD_KAFKA_OFFSET_END);

        $this->assertSame(0, $this->queue->size());
    }

    public function test_pending_size_returns_consumer_lag(): void
    {
        $this->mockQueueOffsets(low: 12, high: 115, committed: 101);

        $this->assertSame(115 - 101, $this->queue->pendingSize());
    }

    public function test_delayed_size_is_zero(): void
    {
        $this->assertSame(0, $this->queue->delayedSize('orders'));
    }

    public function test_reserved_size_is_zero(): void
    {
        $this->assertSame(0, $this->queue->reservedSize('orders'));
    }

    public function test_oldest_pending_job_creation_time_is_unavailable(): void
    {
        $this->assertNull($this->queue->creationTimeOfOldestPendingJob('orders'));
    }

    private function mockQueueOffsets(int $low, int $high, int $committed): void
    {
        $kafkaConsumer = Mockery::mock(KafkaConsumerWrapper::class);
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.kafka_consumer', Mockery::any())
            ->andReturn($kafkaConsumer);
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.conf', Mockery::any())
            ->andReturn(new \RdKafka\Conf);
        $kafkaConsumer
            ->shouldReceive('queryWatermarkOffsets')
            ->andReturnUsing(function ($queue, $partition, &$actualLow, &$actualHigh) use ($low, $high) {
                $actualLow = $low;
                $actualHigh = $high;
            });
        $topicPartition = Mockery::mock(TopicPartitionWrapper::class);
        $topicPartition->shouldReceive('getOffset')->andReturn($committed);
        $kafkaConsumer->shouldReceive('getCommittedOffsets')->andReturn([$topicPartition]);
    }

    public function test_push(): void
    {
        $job = new TestJob;
        $data = [];

        $topic = Mockery::mock(\RdKafka\ProducerTopic::class);
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.producer', Mockery::any())
            ->andReturn($this->producer);
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.conf', Mockery::any())
            ->andReturn(new \RdKafka\Conf);
        $topic->shouldReceive('produce')->andReturnUndefined();
        $this->producer->shouldReceive('newTopic')->andReturn($topic);
        $this->producer->shouldReceive('flush')->andReturn(RD_KAFKA_RESP_ERR_NO_ERROR);

        // normal operation
        $key = $this->queue->push($job, $data);
        $this->assertNotNull($key);
        $this->assertEquals(26, strlen($key));
    }

    public function test_push_uses_job_kafka_key_when_provided(): void
    {
        $job = new TestJobWithKafkaKey('order-42');
        $data = [];

        $topic = Mockery::mock(\RdKafka\ProducerTopic::class);
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.producer', Mockery::any())
            ->andReturn($this->producer);
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.conf', Mockery::any())
            ->andReturn(new \RdKafka\Conf);
        $topic->shouldReceive('produce')
            ->once()
            ->with(RD_KAFKA_PARTITION_UA, 0, Mockery::any(), 'order-42')
            ->andReturnUndefined();
        $this->producer->shouldReceive('newTopic')->andReturn($topic);
        $this->producer->shouldReceive('flush')->andReturn(RD_KAFKA_RESP_ERR_NO_ERROR);

        $key = $this->queue->push($job, $data);
        $this->assertSame('order-42', $key);
    }

    public function test_push_flushes_the_producer_that_accepted_the_message(): void
    {
        $topic = Mockery::mock(\RdKafka\ProducerTopic::class);
        $replacementProducer = Mockery::mock(\RdKafka\Producer::class);

        $this->producer->shouldReceive('newTopic')->once()->andReturn($topic);
        $this->producer->shouldReceive('flush')
            ->once()
            ->with($this->config['timeout_ms'])
            ->andReturn(RD_KAFKA_RESP_ERR_NO_ERROR);
        $replacementProducer->shouldNotReceive('flush');
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.producer', Mockery::any())
            ->andReturn($replacementProducer);
        $topic->shouldReceive('produce')
            ->once()
            ->andReturnUsing(function (): void {
                $this->queue->setConfig(array_merge($this->config, ['brokers' => 'kafka:9092']));
            });

        $this->assertNotNull($this->queue->pushRaw('payload'));
    }

    public function test_make_2nd_try_when_push_error(): void
    {
        $job = new TestJob;
        $data = [];
        $topic = Mockery::mock(\RdKafka\ProducerTopic::class);
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.producer', Mockery::any())
            ->andReturn($this->producer);
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.conf', Mockery::any())
            ->andReturn(new \RdKafka\Conf);
        $topic->shouldReceive('produce')->andReturnUndefined();
        $this->producer->shouldReceive('newTopic')->andReturn($topic);
        $this->producer
            ->shouldReceive('flush')
            ->twice()
            ->andReturnValues([
                RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN,
                RD_KAFKA_RESP_ERR_NO_ERROR,
            ]);
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.producer', Mockery::any())
            ->andReturn($this->producer);

        $key = $this->queue->push($job, $data);
        $this->assertNotNull($key);
        $this->assertEquals(26, strlen($key));
    }

    public function test_push_error_returned(): void
    {
        $job = new TestJob;
        $data = [];
        $topic = Mockery::mock(\RdKafka\ProducerTopic::class);
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.producer', Mockery::any())
            ->andReturn($this->producer);
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.conf', Mockery::any())
            ->andReturn(new \RdKafka\Conf);
        $topic->shouldReceive('produce')->andReturnUndefined();
        $this->producer->shouldReceive('newTopic')->andReturn($topic);
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.producer', Mockery::any())
            ->andReturn($this->producer);
        $this->producer
            ->shouldReceive('flush')
            ->twice()
            ->andReturn(RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN);
        $key = $this->queue->push($job, $data);
        $this->assertNull($key);
    }

    public function test_push_exception(): void
    {
        $job = new TestJob;
        $data = [];
        $topic = Mockery::mock(\RdKafka\ProducerTopic::class);
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.producer', Mockery::any())
            ->andReturn($this->producer);
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.conf', Mockery::any())
            ->andReturn(new \RdKafka\Conf);
        $topic->shouldReceive('produce')
            ->twice()
            ->andThrowExceptions([new QueueKafkaException('dummy exception')]);
        $this->producer->shouldReceive('newTopic')->andReturn($topic);
        $this->container->shouldReceive('makeWith')
            ->with('queue.kafka.producer', Mockery::any())
            ->andReturn($this->producer);
        $this->producer
            ->shouldReceive('flush')
            ->andReturn(RD_KAFKA_RESP_ERR_NO_ERROR);
        $key = $this->queue->push($job, $data);
        $this->assertNull($key);
    }

    public function test_later(): void
    {
        $delay = 5;
        $job = new TestJob;
        $this->expectException(\Exception::class);
        $this->queue->later($delay, $job);
    }

    public function pop_job_with_message_error(int $messageError, bool $consumeStopTriggered = true): ?KafkaJob
    {
        $queue = $this->config['queue'];
        $message = Mockery::mock(\RdKafka\Message::class);
        $message->err = $messageError;
        $message->payload = '{"payload":"payload"}';

        $refGetConsumerTopic = new ReflectionMethod($this->queue, 'getConsumerTopic');
        $topic = $refGetConsumerTopic->invokeArgs($this->queue, [$queue]);
        $refConsumerTopics = new ReflectionProperty($this->queue, '_consumer_topics');
        $this->assertEquals($topic, $refConsumerTopics->getValue($this->queue)[$queue]);
        $topic = Mockery::mock(\RdKafka\ConsumerTopic::class);
        $refConsumerTopics->setValue($this->queue, [$queue => $topic]);

        $topic->shouldNotReceive('consumeStart'); // topic already created
        if ($consumeStopTriggered) {
            $topic->shouldReceive('consumeStop')->once();
        } else {
            $topic->shouldNotReceive('consumeStop');
        }
        $topic->shouldReceive('consume')->once()->andReturn($message);

        return $this->queue->pop($queue);
    }

    public function test_pop_no_error(): void
    {
        $job = $this->pop_job_with_message_error(
            messageError: RD_KAFKA_RESP_ERR_NO_ERROR,
            consumeStopTriggered: false,
        );
        $this->assertEquals('{"payload":"payload"}', json_encode($job->payload()));
    }

    public function test_pop_end_of_partition(): void
    {
        $job = $this->pop_job_with_message_error(
            messageError: RD_KAFKA_RESP_ERR__PARTITION_EOF,
            consumeStopTriggered: false,
        );
        $this->assertNull($job);
    }

    public function test_pop_end_of_partition_stops_consumer_when_configured(): void
    {
        $this->queue->setConfig(array_merge($this->config, ['stop_consume_on_empty' => true]));
        $consumer = new ReflectionProperty($this->queue, '_consumer');
        $consumer->setValue($this->queue, $this->consumer);

        $job = $this->pop_job_with_message_error(
            messageError: RD_KAFKA_RESP_ERR__PARTITION_EOF,
            consumeStopTriggered: true,
        );
        $this->assertNull($job);
    }

    public function test_pop_timed_out(): void
    {
        $job = $this->pop_job_with_message_error(
            messageError: RD_KAFKA_RESP_ERR__TIMED_OUT,
            consumeStopTriggered: true,
        );
        $this->assertNull($job);
    }

    public function test_pop_all_brokers_down(): void
    {
        $job = $this->pop_job_with_message_error(
            messageError: RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN,
            consumeStopTriggered: true,
        );
        $this->assertNull($job);
    }

    public function test_pop_not_catched_exception(): void
    {
        $queue = $this->config['queue'];
        $refGetConsumerTopic = new ReflectionMethod($this->queue, 'getConsumerTopic');
        $topic = $refGetConsumerTopic->invokeArgs($this->queue, [$queue]);
        $refConsumerTopics = new ReflectionProperty($this->queue, '_consumer_topics');
        $this->assertEquals($topic, $refConsumerTopics->getValue($this->queue)[$queue]);
        $topic = Mockery::mock(\RdKafka\ConsumerTopic::class);
        $refConsumerTopics->setValue($this->queue, [$queue => $topic]);

        $topic->shouldNotReceive('consumeStart'); // topic already created
        $topic->shouldReceive('consumeStop')->once();
        $topic->shouldReceive('consume')
            ->once()
            ->andThrowExceptions([new QueueKafkaException('dummy exception')]);
        $job = $this->queue->pop($queue);

        $this->assertNull($job);
    }

    protected function tearDown(): void
    {
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);
        Mockery::close();
        parent::tearDown();
    }
}
