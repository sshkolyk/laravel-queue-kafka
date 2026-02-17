<?php

namespace Rapide\LaravelQueueKafka;

class LumenQueueKafkaServiceProvider extends LaravelQueueKafkaServiceProvider
{
    /**
     * Register the application's event listeners.
     */
    public function boot(): void
    {
        parent::boot();

        $this->mergeConfigFrom(
            __DIR__.'/../config/queue-kafka.php', 'queue.connections.kafka'
        );
    }
}
