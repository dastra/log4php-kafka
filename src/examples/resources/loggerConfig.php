<?php
declare(strict_types = 1);

use amoma\log4phpKafka\appenders\LoggerAppenderKafka;

return [
    'rootLogger' => [
        'appenders' => ['default']
    ],
    'appenders' => [
        'default' => [
            'class' => LoggerAppenderKafka::class,
            'layout' => [
                'class' => 'LoggerLayoutPattern',
                'params' => [
                    'conversionPattern' => '{"timestamp":"%d{Y-m-d H:i:s,u}", "log-level":"%p", "process-id":"%t", "content":%m}%n'
                ]
            ],
            'params' => [
                'topic' => 'default',
                'brokers' => ['localhost:9092']
            ]
        ]
    ]
];
