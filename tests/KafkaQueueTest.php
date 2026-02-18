<?php

namespace Rapide\LaravelQueueKafka\Tests;

use Illuminate\Container\Container;
use Illuminate\Support\Facades\Facade;
use Illuminate\Support\Facades\Log;
use Mockery;
use PHPUnit\Framework\TestCase;
use Rapide\LaravelQueueKafka\Exceptions\QueueKafkaException;
use Rapide\LaravelQueueKafka\Queue\Jobs\KafkaJob;
use Rapide\LaravelQueueKafka\Queue\KafkaQueue;
use Rapide\LaravelQueueKafka\Tests\Jobs\TestJob;
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
            'auto_commit' => 'true',
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

    public function test_size_when_internal_exception(): void
    {
        $size = $this->queue->size();
        $this->assertEquals(0, $size);
    }

    public function test_size_normal(): void
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
            ->andReturnUsing(function ($queue, $partition, &$low, &$high) {
                $low = 11;
                $high = 115;
            });
        $topicPartition = Mockery::mock(TopicPartitionWrapper::class);
        $topicPartition->shouldReceive('getOffset')->andReturn(101);
        $kafkaConsumer->shouldReceive('getCommittedOffsets')->andReturn([$topicPartition]);
        $this->assertEquals(115 - 101, $this->queue->size());
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

    public function test_pop_no_error(): void
    {
        $queue = $this->config['queue'];
        $message = Mockery::mock(\RdKafka\Message::class);
        $message->err = RD_KAFKA_RESP_ERR_NO_ERROR;
        $message->payload = '{"payload":"payload"}';

        $refGetConsumerTopic = new ReflectionMethod($this->queue, 'getConsumerTopic');
        $topic = $refGetConsumerTopic->invokeArgs($this->queue, [$queue]);
        $refConsumerTopics = new ReflectionProperty($this->queue, '_consumer_topics');
        $this->assertEquals($topic, $refConsumerTopics->getValue($this->queue)[$queue]);
        $topic = Mockery::mock(\RdKafka\ConsumerTopic::class);
        $refConsumerTopics->setValue($this->queue, [$queue => $topic]);

        $topic->shouldNotReceive('consumeStart'); // topic already created
        $topic->shouldNotReceive('consumeStop');
        $topic->shouldReceive('consume')->once()->andReturn($message);
        $job = $this->queue->pop($queue);

        $this->assertInstanceOf(KafkaJob::class, $job);
        $this->assertEquals('{"payload":"payload"}', json_encode($job->payload()));
    }

    public function test_pop_end_of_partition(): void
    {
        $queue = $this->config['queue'];
        $message = Mockery::mock(\RdKafka\Message::class);
        $message->err = RD_KAFKA_RESP_ERR__PARTITION_EOF;

        $refGetConsumerTopic = new ReflectionMethod($this->queue, 'getConsumerTopic');
        $topic = $refGetConsumerTopic->invokeArgs($this->queue, [$queue]);
        $refConsumerTopics = new ReflectionProperty($this->queue, '_consumer_topics');
        $this->assertEquals($topic, $refConsumerTopics->getValue($this->queue)[$queue]);
        $topic = Mockery::mock(\RdKafka\ConsumerTopic::class);
        $refConsumerTopics->setValue($this->queue, [$queue => $topic]);

        $topic->shouldNotReceive('consumeStart'); // topic already created
        $topic->shouldReceive('consumeStop')->once();
        $topic->shouldReceive('consume')->once()->andReturn($message);
        $job = $this->queue->pop($queue);

        $this->assertNull($job);
    }

    public function test_pop_timed_out(): void
    {
        $queue = $this->config['queue'];
        $message = Mockery::mock(\RdKafka\Message::class);
        $message->err = RD_KAFKA_RESP_ERR__TIMED_OUT;

        $refGetConsumerTopic = new ReflectionMethod($this->queue, 'getConsumerTopic');
        $topic = $refGetConsumerTopic->invokeArgs($this->queue, [$queue]);
        $refConsumerTopics = new ReflectionProperty($this->queue, '_consumer_topics');
        $this->assertEquals($topic, $refConsumerTopics->getValue($this->queue)[$queue]);
        $topic = Mockery::mock(\RdKafka\ConsumerTopic::class);
        $refConsumerTopics->setValue($this->queue, [$queue => $topic]);

        $topic->shouldNotReceive('consumeStart'); // topic already created
        $topic->shouldReceive('consumeStop')->once();
        $topic->shouldReceive('consume')->once()->andReturn($message);
        $job = $this->queue->pop($queue);

        $this->assertNull($job);
    }

    public function test_pop_not_catched_exception(): void
    {
        $queue = $this->config['queue'];
        $message = Mockery::mock(\RdKafka\Message::class);
        $message->err = RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN;
        $message->shouldReceive('errstr');

        $refGetConsumerTopic = new ReflectionMethod($this->queue, 'getConsumerTopic');
        $topic = $refGetConsumerTopic->invokeArgs($this->queue, [$queue]);
        $refConsumerTopics = new ReflectionProperty($this->queue, '_consumer_topics');
        $this->assertEquals($topic, $refConsumerTopics->getValue($this->queue)[$queue]);
        $topic = Mockery::mock(\RdKafka\ConsumerTopic::class);
        $refConsumerTopics->setValue($this->queue, [$queue => $topic]);

        $topic->shouldNotReceive('consumeStart'); // topic already created
        $topic->shouldReceive('consumeStop')->once();
        $topic->shouldReceive('consume')->once()->andReturn($message);
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
