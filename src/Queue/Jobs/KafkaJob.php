<?php

namespace Rapide\LaravelQueueKafka\Queue\Jobs;

use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\Jobs\Job;
use Rapide\LaravelQueueKafka\Exceptions\QueueKafkaException;
use RdKafka\ConsumerTopic;
use RdKafka\Message;

class KafkaJob extends Job implements JobContract
{
    public function __construct(
        protected $container,
        protected Queue $connection,
        protected Message $message,
        protected $connectionName,
        protected $queue,
        protected ConsumerTopic $topic
    ) {}

    /**
     * Get the number of times the job has been attempted.
     */
    public function attempts(): int
    {
        return 1; // retries not supported
    }

    /**
     * Get the raw body string for the job.
     */
    public function getRawBody(): string
    {
        return $this->message->payload;
    }

    /**
     * Delete the job from the queue.
     */
    public function delete(): void
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
     */
    public function getJobId(): string
    {
        return $this->message->key;
    }

    public function uuid(): string
    {
        return $this->message->key;
    }
}
