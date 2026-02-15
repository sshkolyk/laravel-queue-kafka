<?php

namespace Rapide\LaravelQueueKafka\Queue;

use Illuminate\Contracts\Container\BindingResolutionException;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Illuminate\Support\Facades\App;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;
use Rapide\LaravelQueueKafka\Exceptions\QueueKafkaException;
use Rapide\LaravelQueueKafka\Queue\Jobs\KafkaJob;
use RdKafka\Consumer;
use RdKafka\ConsumerTopic;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;
use RdKafka\ProducerTopic;
use RdKafka\TopicPartition;

class KafkaQueue extends Queue implements QueueContract
{
    protected string $defaultQueue;

    protected ?int $sleepOnError = null;

    protected ?Producer $_producer = null;

    protected ?\RdKafka\Conf $_consumer_conf = null;

    protected ?Consumer $_consumer = null;

    /** @var array<ProducerTopic> */
    protected array $_producer_topics = [];

    /** @var array<ConsumerTopic> */
    protected array $_consumer_topics = [];

    public function __construct(array $config)
    {
        $this->defaultQueue = $config['queue'];
        if (@$config['sleep_on_error']) {
            $this->sleepOnError = $config['sleep_on_error'];
        }
        $this->setConfig($config);
    }

    /**
     * Get the size of the queue.
     *
     * @param  null|string  $queue
     *
     * @throws BindingResolutionException
     */
    public function size($queue = null): int
    {
        $queue = $this->getQueueName($queue);
        try {
            $kafkaConsumer = $this->getKafkaConsumer();
            $kafkaConsumer->queryWatermarkOffsets(
                $queue,
                $this->getConfig()['consumer_partition'],
                $low,
                $high,
                $this->getConfig()['timeout_ms'],
            );
            $topicPartition = new TopicPartition($queue, $this->getConfig()['consumer_partition']);
            $offsets = $kafkaConsumer->getCommittedOffsets([$topicPartition], $this->getConfig()['timeout_ms']);
            $offset = $offsets[0]->getOffset();
            if ($offset < 0) { // when get RD_KAFKA_OFFSET_...
                return 0;
            }

            return $high - $offset;
        } catch (\Throwable $exception) {
            Log::error('Kafka error while attempting size(): '.$exception->getMessage());

            return 0;
        }
    }

    /**
     * Push a new job onto the queue.
     *
     * @param  string  $job
     * @param  mixed  $data
     * @param  string  $queue
     */
    public function push($job, $data = '', $queue = null): bool
    {
        return $this->pushRaw($this->createPayload($job, $queue, $data), $queue, []);
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param  mixed  $payload
     * @param  ?string  $queue
     *
     * @throws QueueKafkaException
     */
    public function pushRaw($payload, $queue = null, array $options = []): ?string
    {
        try {
            $topic = $this->getProducerTopic($queue);
            $key = Str::upper((string) Str::ulid());
            $topic->produce(RD_KAFKA_PARTITION_UA, 0, $payload, $key);
            if ($result = $this->getProducer()->flush($this->getConfig()['timeout_ms'])) {
                $this->reportConnectionError(
                    'pushRaw',
                    new QueueKafkaException('Kafka flush error #'.$result)
                );
            }

            return $key;
        } catch (\Throwable $exception) {
            $this->reportConnectionError('pushRaw', $exception);

            return null;
        }
    }

    /**
     * Push a new job onto the queue after a delay.
     *
     * @param  \DateTime|int  $delay
     * @param  string  $job
     * @param  mixed  $data
     * @param  string  $queue
     *
     * @throws QueueKafkaException
     */
    public function later($delay, $job, $data = '', $queue = null): mixed
    {
        // Later is not supported
        throw new QueueKafkaException('Later not yet implemented');
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param  string|null  $queue
     *
     * @throws QueueKafkaException|BindingResolutionException
     */
    public function pop($queue = null): ?KafkaJob
    {
        try {
            $queue = $this->getQueueName($queue);
            $topic = $this->getConsumerTopic($queue);
            $message = $topic->consume($this->getConfig()['consumer_partition'], $this->getConfig()['timeout_ms']);
            if ($message === null) {
                $this->stopConsumeTopic($queue);

                return null;
            }

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    return new KafkaJob(
                        container: $this->container,
                        connection: $this,
                        message: $message,
                        connectionName: $this->connectionName,
                        queue: $queue ?: $this->defaultQueue,
                        topic: $topic,
                    );
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    $this->stopConsumeTopic($queue);

                    return null;
                default:
                    $this->stopConsumeTopic($queue);
                    throw new QueueKafkaException($message->errstr(), $message->err);
            }
        } catch (\RdKafka\Exception $exception) {
            $this->stopConsumeTopic($queue);
            throw new QueueKafkaException('Could not pop from the queue', 0, $exception);
        }
    }

    protected function getQueueName(?string $queue = null): string
    {
        return $queue ?: $this->defaultQueue;
    }

    /**
     * Return a Kafka producer Topic based on the name
     */
    protected function getProducerTopic(?string $queue = null): ProducerTopic
    {
        $queue = $this->getQueueName($queue);
        if (! array_key_exists($queue, $this->_producer_topics)) {
            $this->_producer_topics[$queue] = $this->getProducer()->newTopic($queue);
        }

        return $this->_producer_topics[$queue];
    }

    /**
     * @throws BindingResolutionException
     */
    protected function getConsumerTopic(?string $queue = null): ConsumerTopic
    {
        $queue = $this->getQueueName($queue);
        if (! array_key_exists($queue, $this->_consumer_topics)) {
            try {
                $this->_consumer_topics[$queue] = $this->getConsumer()->newTopic($queue);
            } catch (BindingResolutionException $e) {
                $this->reportConnectionError('getConsumerTopic', $e);
                throw $e;
            }
            $this->_consumer_topics[$queue]->consumeStart(
                $this->getConfig()['consumer_partition'],
                RD_KAFKA_OFFSET_STORED
            );
        }

        return $this->_consumer_topics[$queue];
    }

    protected function stopConsumeTopic(?string $queue = null): void
    {
        $queue = $this->getQueueName($queue);
        if (array_key_exists($queue, $this->_consumer_topics)) {
            $this->_consumer_topics[$queue]->consumeStop($this->getConfig()['consumer_partition']);
            unset($this->_consumer_topics[$queue]);
        }
    }

    /**
     * @throws QueueKafkaException
     */
    protected function reportConnectionError(string $action, \Throwable $e): void
    {
        Log::error('Kafka error while attempting '.$action.': '.$e->getMessage());

        // If it's set to false, throw an error rather than waiting
        if (! $this->sleepOnError) {
            throw new QueueKafkaException('Error Kafka connection');
        }

        // Sleep so that we don't flood the log file
        sleep($this->sleepOnError);
    }

    /**
     * @throws BindingResolutionException
     */
    protected function getConsumerConfig(): \RdKafka\Conf
    {
        if ($this->_consumer_conf === null) {
            /** @var \RdKafka\TopicConf $topicConf */
            $topicConf = App::makeWith('queue.kafka.topic_conf', []);
            $topicConf->set('auto.offset.reset', $this->getConfig()['auto_offset_reset']);

            $this->_consumer_conf = App::makeWith('queue.kafka.conf', []);
            $this->_consumer_conf->set('bootstrap.servers', $this->getConfig()['brokers']);
            if ($this->getConfig()['sasl_enable'] === true) {
                $this->_consumer_conf->set('sasl.mechanism', $this->getConfig()['sasl_mechanism']);
                $this->_consumer_conf->set('security.protocol', $this->getConfig()['sasl_security_protocol']);
                $this->_consumer_conf->set('sasl.username', $this->getConfig()['sasl_plain_username']);
                $this->_consumer_conf->set('sasl.password', $this->getConfig()['sasl_plain_password']);
                $this->_consumer_conf->set('ssl.ca.location', $this->getConfig()['ssl_ca_location']);
            }
            $this->_consumer_conf->set('group.id', $this->getConfig()['consumer_group_id']);
            $this->_consumer_conf->set('metadata.broker.list', $this->getConfig()['brokers']);
            $this->_consumer_conf->set('enable.auto.commit', $this->getConfig()['auto_commit']);
            $this->_consumer_conf->setDefaultTopicConf($topicConf);
        }

        return $this->_consumer_conf;
    }

    /**
     * @throws BindingResolutionException
     */
    protected function getConsumer(): Consumer
    {
        if (! $this->_consumer) {
            $this->_consumer = $this->container->makeWith('queue.kafka.consumer', ['conf' => $this->getConsumerConfig()]);
        }

        return $this->_consumer;
    }

    /**
     * @throws BindingResolutionException
     */
    protected function getKafkaConsumer(): KafkaConsumer
    {
        return $this->container->makeWith('queue.kafka.kafka_consumer', ['conf' => $this->getConsumerConfig()]);
    }

    /**
     * Returns Kafka Producer
     */
    protected function getProducer(): Producer
    {
        if (! $this->_producer) {
            /** @var \RdKafka\Conf $producerConf */
            $producerConf = App::makeWith('queue.kafka.conf', []);
            $producerConf->set('bootstrap.servers', $this->getConfig()['brokers']);
            $producerConf->set('metadata.broker.list', $this->getConfig()['brokers']);
            $producerConf->set('partitioner', $this->getConfig()['producer_partitioner']);
            if ($this->getConfig()['sasl_enable'] === true) {
                $producerConf->set('sasl.mechanism', $this->getConfig()['sasl_mechanism']);
                $producerConf->set('sasl.mechanism', $this->getConfig()['sasl_mechanism']);
                $producerConf->set('security.protocol', $this->getConfig()['sasl_security_protocol']);
                $producerConf->set('sasl.username', $this->getConfig()['sasl_plain_username']);
                $producerConf->set('sasl.password', $this->getConfig()['sasl_plain_password']);
                $producerConf->set('ssl.ca.location', $this->getConfig()['ssl_ca_location']);
            }
            $topicConf = App::makeWith('queue.kafka.topic_conf', []);
            $topicConf->set('request.timeout.ms', $this->getConfig()['timeout_ms']);
            $producerConf->setDefaultTopicConf($topicConf);
            /** @var \RdKafka\Producer $producer */
            $this->_producer = new \RdKafka\Producer($producerConf);
        }

        return $this->_producer;
    }
}
