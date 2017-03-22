<?php
declare(strict_types = 1);

require_once dirname(__FILE__) . '/../../../vendor/autoload.php';
$loggerConfig = require_once dirname(__FILE__) . '/../resources/loggerConfig.php';

Logger::configure($loggerConfig);
$logger = Logger::getRootLogger();
$logger->fatal("Hello World!");
