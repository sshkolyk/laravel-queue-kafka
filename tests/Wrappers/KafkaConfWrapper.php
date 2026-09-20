<?php

namespace Rapide\LaravelQueueKafka\Tests\Wrappers;

class KafkaConfWrapper extends \RdKafka\Conf
{
    /** @var array<string, string> */
    public array $settings = [];

    public function set(string $name, string $value): void
    {
        $this->settings[$name] = $value;

        parent::set($name, $value);
    }
}
