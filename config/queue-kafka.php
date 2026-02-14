<?php

/**
 * This is an example of queue connection configuration.
 * It will be merged into config/queue.php.
 * You need to set proper values in `.env`.
 */
return [
    /*
     * Driver name
     */
    'driver' => 'kafka',

    /*
     * The name of default queue.
     */
    'queue' => env('KAFKA_QUEUE', 'default'),

    /*
     * The group of where the consumer in resides.
     */
    'consumer_group_id' => env('KAFKA_CONSUMER_GROUP_ID', 'laravel_queue'),

    /*
     *
     */
    'consumer_partition' => env('KAFKA_CONSUMER_PARTITION', 0),

    /*
     * Producer partitioner algo
     * Can be:
     * random - random distribution, consistent - CRC32 hash of key (Empty and NULL keys are mapped to single partition),
     * consistent_random - CRC32 hash of key (Empty and NULL keys are randomly partitioned),
     * murmur2 - Java Producer compatible Murmur2 hash of key (NULL keys are mapped to single partition),
     * murmur2_random - Java Producer compatible Murmur2 hash of key (NULL keys are randomly partitioned.
     *      This is functionally equivalent to the default partitioner in the Java Producer.),
     * fnv1a - FNV-1a hash of key (NULL keys are mapped to single partition),
     * fnv1a_random - FNV-1a hash of key (NULL keys are randomly partitioned).
     */
    'producer_partitioner' => env('KAFKA_PRODUCER_PARTITIONER', 'murmur2_random'),

    /*
     * Address of the Kafka broker
     * Comma-separated list of Kafka broker addresses the client will initially connect to.
     * The client uses these brokers to discover the full Kafka cluster topology.
     */
    'brokers' => env('KAFKA_BROKERS', 'localhost:9092'),

    /*
     * Determine the number of seconds to sleep if there's an error communicating with kafka
     * If set to false, it'll throw an exception rather than doing the sleep for X seconds.
     */
    'sleep_on_error' => env('KAFKA_ERROR_SLEEP', 5),

    /*
     * sasl authorization
     */
    'sasl_enable' => env('KAFKA_SASL_ENABLE', false),

    /*
     * File or directory path to CA certificate(s) for verifying the broker's key. example: storage_path('kafka.client.truststore.jks')
     */
    'ssl_ca_location' => env('KAFKA_SSL_CA_LOCATION', ''),

    /*
     * SASL username for use with the PLAIN and SASL-SCRAM-.. mechanisms
     */
    'sasl_plain_username' => env('KAFKA_SASL_PLAIN_USERNAME'),

    /*
     * SASL password for use with the PLAIN and SASL-SCRAM-.. mechanism
     */
    'sasl_plain_password' => env('KAFKA_SASL_PLAIN_PASSWORD'),

    /*
     * The property enable.auto.commit is set to true by default, and Kafka commits the current offset back to the
     *     Kafka broker at a specified interval, controlled by the auto.commit.interval.ms setting (default is 5000 ms).
    */
    'auto_commit' => env('KAFKA_AUTO_COMMIT', 'true'),

    /**
     * What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted):
     * earliest: automatically reset the offset to the earliest offset
     * latest: automatically reset the offset to the latest offset
     * none: throw exception to the consumer if no previous offset is found for the consumer's group
     * anything else: throw exception to the consumer
     */
    'auto_offset_reset' => env('KAFKA_AUTO_RESET', 'earliest'),

    /** All operations timeout in ms */
    'timeout_ms' => env('KAFKA_TIMEOUT_MS', 1000),
];
