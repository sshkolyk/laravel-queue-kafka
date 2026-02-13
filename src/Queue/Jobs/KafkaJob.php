<?php

namespace Rapide\LaravelQueueKafka\Queue\Jobs;

use Exception;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;
use Illuminate\Queue\Jobs\JobName;
use Illuminate\Support\Str;
use Random\RandomException;
use Rapide\LaravelQueueKafka\Exceptions\QueueKafkaException;
use Rapide\LaravelQueueKafka\Queue\KafkaQueue;
use RdKafka\ConsumerTopic;
use RdKafka\Message;
use Throwable;

class KafkaJob extends Job implements JobContract
{

    /**
     * @var KafkaQueue
     */
    protected $connection;
    /**
     * @var KafkaQueue
     */
    protected $queue;
    /**
     * @var Message
     */
    protected $message;

    /**
     * @var ConsumerTopic
     */
    protected $topic;

    /**
     * KafkaJob constructor.
     *
     * @param Container $container
     * @param KafkaQueue $connection
     * @param Message $message
     * @param string $connectionName
     * @param string $queue
     * @param ConsumerTopic $topic
     */
    public function __construct(
        Container     $container,
        KafkaQueue    $connection,
        Message       $message,
        string        $connectionName,
        string        $queue,
        ConsumerTopic $topic
    )
    {
        $this->container = $container;
        $this->connection = $connection;
        $this->message = $message;
        $this->connectionName = $connectionName;
        $this->queue = $queue;
        $this->topic = $topic;
    }

    /**
     * Get the number of times the job has been attempted.
     *
     * @return int
     */
    public function attempts(): int
    {
        return (int)($this->payload()['attempts']) + 1;
    }

    /**
     * Get the raw body string for the job.
     *
     * @return string
     */
    public function getRawBody()
    {
        return $this->message->payload;
    }

    /**
     * Delete the job from the queue.
     */
    public function delete()
    {
        try {
            parent::delete();
            $this->topic->offsetStore($this->message->partition, $this->message->offset);
        } catch (\RdKafka\Exception $exception) {
            throw new QueueKafkaException('Could not delete job from the queue', 0, $exception);
        }
    }

    /**
     * Get the job identifier.
     *
     * @return string
     */
    public function getJobId(): string
    {
        return $this->message->key;
    }

    /**
     * Sets the job identifier.
     *
     * @param string $id
     */
    public function setJobId(string $id): void
    {
        $this->connection->setCorrelationId($id);
    }
}
