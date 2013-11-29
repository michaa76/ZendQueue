<?php
/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2012 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 * @package   Zend_Queue
 */

namespace ZendQueueTest\Adapter;

use ZendQueue\Adapter;

/**
 * @category   Zend
 * @package    ZendQueue\Queue
 * @subpackage UnitTests
 */
class AmqpTest extends AdapterTest
{
    /**
     * (non-PHPdoc)
     * @see \ZendQueueTest\Adapter\AdapterTest::getAdapterName()
     */
    public function getAdapterName()
    {
        return 'Amqp';
    }

    /**
     * (non-PHPdoc)
     * @see \ZendQueueTest\Adapter\AdapterTest::getAdapterFullName()
     */
    public function getAdapterFullName()
    {
        return '\ZendQueue\Adapter\\' . $this->getAdapterName();
    }

    public function getTestConfig()
    {
        $driverOptions = array();
        if (defined('TESTS_ZEND_QUEUE_AMQP_HOST')) {
            $driverOptions['host'] = TESTS_ZEND_QUEUE_AMQP_HOST;
        }
        if (defined('TESTS_ZEND_QUEUE_AMQP_PORT')) {
            $driverOptions['port'] = TESTS_ZEND_QUEUE_AMQP_PORT;
        }
        if (defined('TESTS_ZEND_QUEUE_AMQP_USER')) {
            $driverOptions['user'] = TESTS_ZEND_QUEUE_AMQP_USER;
        }
        if (defined('TESTS_ZEND_QUEUE_AMQP_PASS')) {
            $driverOptions['pass'] = TESTS_ZEND_QUEUE_AMQP_PASS;
        }
        if (defined('TESTS_ZEND_QUEUE_AMQP_VHOST')) {
            $driverOptions['vhost'] = TESTS_ZEND_QUEUE_AMQP_VHOST;
        }
        if (defined('TESTS_ZEND_QUEUE_AMQP_DEBUG')) {
            $driverOptions['debug'] = TESTS_ZEND_QUEUE_AMQP_DEBUG;
        }
        return array('driverOptions' => $driverOptions);
    }

    public function testFailedConstructor()
    {
//         $queue = $this->createQueue(__FUNCTION__, array());
        try {
            $queue = $this->createQueue(__FUNCTION__, array());
            $this->fail('The test should fail if no host and password are passed');
        } catch (\Exception $e) {
            $this->assertTrue( true, 'Job Queue host and password should be provided');
        }
    }

    public function testDelete()
    {
        if (!$queue = $this->createQueue(__FUNCTION__)) {
            return;
        }
        $adapter = $queue->getAdapter();
// var_dump(__CLASS__.__FUNCTION__.__LINE__, get_class($adapter));
        $new = $this->createQueueName(__FUNCTION__ . '_2');
        $this->assertTrue($adapter->create($new));
        $this->assertTrue($adapter->delete($new));
    }

    /**
     * (non-PHPdoc)
     * @see \ZendQueueTest\Adapter\AdapterTest::testReceive()
     */
    public function testReceive()
    {
        if (!$queue = $this->createQueue(__FUNCTION__)) {
            return;
        }
        $adapter = $queue->getAdapter();

        // send the message
//         $body = 'this is a test message 2';
//         $message = $adapter->send($body);
//         $this->assertTrue($message instanceof Message);

//         if (!$queue = $this->createQueue(__FUNCTION__)) {
//             return;
//         }
//         $adapter = $queue->getAdapter();

        // get it back
        $list = $adapter->receive(1);
    }

    /**
     * (non-PHPdoc)
     * @see \ZendQueueTest\Adapter\AdapterTest::testDeleteMessage()
     */
    public function testDeleteMessage()
    {
        if (!$queue = $this->createQueue(__FUNCTION__)) {
            return;
        }
        $adapter = $queue->getAdapter();

        $body = 'this is a test message';
        $message = $adapter->send($body);

        $this->assertTrue($adapter->deleteMessage($message));
    }
}