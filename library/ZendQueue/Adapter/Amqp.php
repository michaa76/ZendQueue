<?php
/**
 * Zend Framework (http://framework.zend.com/)
 *
 * @link      http://github.com/zendframework/zf2 for the canonical source repository
 * @copyright Copyright (c) 2005-2013 Zend Technologies USA Inc. (http://www.zend.com)
 * @license   http://framework.zend.com/license/new-bsd New BSD License
 */

namespace ZendQueue\Adapter;

use ZendQueue\Exception;
use ZendQueue\Message;
use ZendQueue\Queue;

use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Message\AMQPMessage;


/**
 * Class for using AMQP to talk to an AMQP compliant server
 *
 * @category   Zend
 * @package    ZendQueue\Queue
 * @subpackage Adapter
 */

class Amqp extends AbstractAdapter
{
    const DEFAULT_HOST = 'localhost';
    const DEFAULT_PORT = 8672;
    const DEFAULT_USER = 'guest';
    const DEFAULT_PASS = 'guest';
    const DEFAULT_VHOST = '/';
    const DEFAULT_DEBUG = false;

    /**
     * @var \PhpAmqpLib\Connection\AMQPConnection
     */
    private $conn = null;

    /**
     *
     * @var \PhpAmqpLib\Connection\AMQPChannel
     */
    private $ch = null;

    /**
     *
     * @var string
     */
    private $exchange = 'router';

    /**
     *
     * @var string
     */
    private $consumerTag = 'consumer';

    /**
     * Constructor
     *
     * @param  array|\Traversable $options An array having configuration data
     * @param  \ZendQueue\Queue The \ZendQueue\Queue object that created this class
     */
    public function __construct($options, Queue $queue = null)
    {
        parent::__construct($options, $queue);
        $options = &$this->_options['driverOptions'];
        if (!array_key_exists('host', $options)) {
            $options['host'] = self::DEFAULT_HOST;
        }
        if (!array_key_exists('port', $options)) {
            $options['port'] = self::DEFAULT_PORT;
        }
        if (!array_key_exists('user', $options)) {
            $options['user'] = self::DEFAULT_USER;
        }
        if (!array_key_exists('pass', $options)) {
            $options['pass'] = self::DEFAULT_PASS;
        }
        if (!array_key_exists('vhost', $options)) {
            $options['vhost'] = self::DEFAULT_VHOST;
        }
        if (!array_key_exists('debug', $options)) {
            $options['debug'] = self::DEFAULT_DEBUG;
        }
        defined('AMQP_DEBUG') ? AMQP_DEBUG : $options['debug'];
        $this->initAdapter();
    }

    /**
     * Initialize AMQP adapter
     */
    protected function initAdapter()
    {
        $options = &$this->_options['driverOptions'];
        $this->conn = new AMQPConnection($options['host'], $options['port'], $options['user'], $options['pass'], $options['vhost']);
        $this->ch = $this->conn->channel();
    }

    /**
     * Close the socket explicitly when destructed
     *
     * @return void
     */
    public function __destruct()
    {
//         $this->ch->close();
//         $this->conn->close();
    }

    /**
     * Retrieve queue instance
     *
     * @return \ZendQueue\Queue
     */
//     public function getQueue()
//     {
//         throw new Exception\UnsupportedMethodCallException(__FUNCTION__ . '() is not supported in this adapter');
//     }

    /**
     * Set queue instnace
     *
     * @param  \ZendQueue\Queue $queue
     * @return \ZendQueue\Adapter
    */
//     public function setQueue(Queue $queue)
//     {
//     }

    /**
     * Does a queue already exist?
     *
     * Use isSupported('isExists') to determine if an adapter can test for
     * queue existance.
     *
     * @param  string $name Queue name
     * @return boolean
    */
    public function isExists($name)
    {
//         throw new Exception\UnsupportedMethodCallException(__FUNCTION__ . '() is not supported in this adapter');
//         return true;
        if (empty($this->_queues)) {
            $this->getQueues();
        }
        return in_array($name, $this->_queues);
    }

    /**
     * Create a new queue
     *
     * Visibility timeout is how long a message is left in the queue
     * "invisible" to other readers.  If the message is acknowleged (deleted)
     * before the timeout, then the message is deleted.  However, if the
     * timeout expires then the message will be made available to other queue
     * readers.
     *
     * @param  string  $name Queue name
     * @param  integer $timeout Default visibility timeout
     * @return boolean
    */
    public function create($name, $timeout=null)
    {
        try {
            $this->ch->queue_declare($name, false, true, false, false);
            $this->ch->exchange_declare($this->exchange, 'direct', false, true, false);
            $this->ch->queue_bind($name, $this->exchange);
            $this->_queues[] = $name;
            return true;
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage(), $e->getCode(), $e);
        }
        return false;
    }

    /**
     * Delete a queue and all of its messages
     *
     * Return false if the queue is not found, true if the queue exists.
     *
     * @param  string $name Queue name
     * @return boolean
    */
    public function delete($name)
    {
        try {
            $this->ch->queue_unbind($name, $this->exchange);
            $this->ch->exchange_delete($this->exchange);
//             $this->ch->queue_delete($name, false, true, false, false);
            $this->ch->queue_delete($name, true, false, false, false);
            $key = array_search($name, $this->_queues);
            if ($key !== false) {
                unset($this->_queues[$key]);
            }
            return true;
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage(), $e->getCode(), $e);
        }
        return false;
    }

    /**
     * Get an array of all available queues
     *
     * Not all adapters support getQueues(); use isSupported('getQueues')
     * to determine if the adapter supports this feature.
     *
     * @return array
    */
    public function getQueues()
    {
        $list = array_keys($this->_queues);
        return $list;
    }

    /**
     * Return the approximate number of messages in the queue
     *
     * @param  \ZendQueue\Queue|null $queue
     * @return integer
    */
    public function count(Queue $queue = null)
    {
        throw new Exception\UnsupportedMethodCallException(__FUNCTION__ . '() is not supported in this adapter');
    }

    /********************************************************************
     * Messsage management functions
    *********************************************************************/

    /**
     * Send a message to the queue
     *
     * @param  mixed $message Message to send to the active queue
     * @param  \ZendQueue\Queue|null $queue
     * @return \ZendQueue\Message
    */
    public function send($message, Queue $queue = null)
    {
        if ($queue === null) {
            $queue = $this->_queue;
        }
        if (is_scalar($message)) {
            $message = (string) $message;
        }
        if (is_string($message)) {
            $message = trim($message);
        }
        $msg = new AMQPMessage($message);
        try {
            $this->ch->basic_publish($msg, $this->exchange);
        } catch (\Exception $e) {
            throw new Exception\RuntimeException($e->getMessage(), $e->getCode(), $e);
        }
        $data    = array(
            'message_id' => md5(uniqid(rand(), true)),
//             'handle'     => null,
            'handle'     => md5(uniqid(rand(), true)),
            'body'       => $message,
            'md5'        => md5($message),
        );
        $options = array(
            'queue' => $queue,
            'data'  => $data,
        );
        $classname = $queue->getMessageClass();
        return new $classname($options);
    }

    /**
     * Get messages in the queue
     *
     * @param  integer|null $maxMessages Maximum number of messages to return
     * @param  integer|null $timeout Visibility timeout for these messages
     * @param  \ZendQueue\Queue|null $queue
     * @return \ZendQueue\Message\MessageIterator
    */
    public function receive($maxMessages = null, $timeout = null, Queue $queue = null)
    {
        if ($maxMessages === null) {
            $maxMessages = 1;
        }
        if ($timeout === null) {
            $nowait = true;
        }
        if ($queue === null) {
            $queue = $this->_queue;
        }
        $i = 0;
        $this->msgs = array();
        if ($maxMessages > 0 ) {
            $this->ch->basic_consume($queue->getName(), $this->consumerTag, false, $no_ack=false, $exclusive=false, $nowait, $callback=array($this, 'processMsg'));
            while (count($this->ch->callbacks) && $i < $maxMessages) {
                $this->ch->wait();
                $i++;
            }
        }
        $options = array(
            'queue'        => $queue,
            'data'         => $this->msgs,
            'messageClass' => $queue->getMessageClass(),
        );
        $classname = $queue->getMessageSetClass();
        return new $classname($options);
    }

    /**
     *
     * @param PhpAmqpLib\Message\AMQPMessage $msg
     */
    public function processMsg($msg)
    {
        $data = array(
            'handle' => md5(uniqid(rand(), true)),
            'body'   => (string)$msg->body,
        );
        $this->msgs[] = $data;
        /*
         * Send a message with the string "quit" to cancel the consumer.
         */
        if ($msg->body === 'quit') {
            $msg->delivery_info['channel']->basic_cancel($msg->delivery_info['consumer_tag']);
        }
    }

    /**
     * Delete a message from the queue
     *
     * Return true if the message is deleted, false if the deletion is
     * unsuccessful.
     *
     * @param  \ZendQueue\Message $message
     * @return boolean
    */
    public function deleteMessage(Message $message)
    {
var_dump(__CLASS__.__FUNCTION__.__LINE__, $message->handle);
die(__CLASS__.__FUNCTION__.__LINE__);
//         $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
//         throw new Exception\UnsupportedMethodCallException(__FUNCTION__ . '() is not supported in this adapter');
    }

    /********************************************************************
     * Supporting functions
    *********************************************************************/

    /**
     * Returns the configuration options in this adapter.
     *
     * @return array
    */
    public function getOptions()
    {
        return parent::getOptions();
    }

    /**
     * Return a list of queue capabilities functions
     *
     * $array['function name'] = true or false
     * true is supported, false is not supported.
     *
     * @return array
    */
    public function getCapabilities()
    {
        return array(
                'create'        => true,
                'delete'        => true,
                'send'          => true,
                'receive'       => true,
                'deleteMessage' => true,
                'getQueues'     => true,
                'count'         => true,
                'isExists'      => true,
        );
    }
}
