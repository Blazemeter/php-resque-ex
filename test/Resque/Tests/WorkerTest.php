<?php

use MonologInit\MonologInit;

require_once dirname(__FILE__) . DIRECTORY_SEPARATOR . 'bootstrap.php';

/**
 * Resque_Worker tests.
 *
 * @package     Resque/Tests
 * @author      Chris Boulton <chris@bigcommerce.com>
 * @license     http://www.opensource.org/licenses/mit-license.php
 */
class Resque_Tests_WorkerTest extends Resque_Tests_TestCase
{
    public function testWorkerRegistersInList()
    {
        $worker = new Resque_Worker('*');
        $worker->registerWorker();

        // Make sure the worker is in the list
        $this->assertTrue((bool)$this->redis->sismember(Resque::WORKERS, (string)$worker));
    }

    public function testGetAllWorkers()
    {
        $num = 3;
        // Register a few workers
        for($i = 0; $i < $num; ++$i) {
            $worker = new Resque_Worker('queue_' . $i);
            $worker->registerWorker();
            $worker->registerLogger(new MonologInit('', ''));
        }

        // Now try to get them
        $this->assertEquals($num, count(Resque_Worker::all()));
    }

    public function testGetWorkerById()
    {
        $worker = new Resque_Worker('*');
        $worker->registerWorker();
        $worker->registerLogger(new MonologInit('', ''));

        $newWorker = Resque_Worker::find((string)$worker);
        $this->assertEquals((string)$worker, (string)$newWorker);
    }

    public function testInvalidWorkerDoesNotExist()
    {
        $this->assertFalse(Resque_Worker::exists('blah'));
    }

    public function testWorkerCanUnregister()
    {
        $worker = new Resque_Worker('*');
        $worker->registerWorker();
        $worker->unregisterWorker();

        $this->assertFalse(Resque_Worker::exists((string)$worker));
        $this->assertEquals(array(), Resque_Worker::all());
        $this->assertEquals(array(), $this->redis->smembers('resque:workers'));
    }

    public function testPausedWorkerDoesNotPickUpJobs()
    {
        $worker = new Resque_Worker('*');
        $worker->pauseProcessing();
        Resque::enqueue('jobs', 'Test_Job');
        $worker->work(0);
        $worker->work(0);
        $this->assertEquals(0, Resque_Stat::get(Resque::PROCESSED));
    }

    public function testResumedWorkerPicksUpJobs()
    {
        $worker = new Resque_Worker('*');
        $worker->pauseProcessing();
        Resque::enqueue('jobs', 'Test_Job');
        $worker->work(0);
        $this->assertEquals(0, Resque_Stat::get(Resque::PROCESSED));
        $worker->unPauseProcessing();
        $worker->work(0);
        $this->assertEquals(1, Resque_Stat::get(Resque::PROCESSED));
    }

    public function testWorkerCanWorkOverMultipleQueues()
    {
        $worker = new Resque_Worker(array(
            'queue1',
            'queue2'
        ));
        $worker->registerWorker();
        Resque::enqueue('queue1', 'Test_Job_1');
        Resque::enqueue('queue2', 'Test_Job_2');

        $job = $worker->reserve();
        $this->assertEquals('queue1', $job->queue);

        $job = $worker->reserve();
        $this->assertEquals('queue2', $job->queue);
    }

    public function testWorkerWorksQueuesInSpecifiedOrder()
    {
        $worker = new Resque_Worker(array(
            'high',
            'medium',
            'low'
        ));
        $worker->registerWorker();

        // Queue the jobs in a different order
        Resque::enqueue('low', 'Test_Job_1');
        Resque::enqueue('high', 'Test_Job_2');
        Resque::enqueue('medium', 'Test_Job_3');

        // Now check we get the jobs back in the right order
        $job = $worker->reserve();
        $this->assertEquals('high', $job->queue);

        $job = $worker->reserve();
        $this->assertEquals('medium', $job->queue);

        $job = $worker->reserve();
        $this->assertEquals('low', $job->queue);
    }

    public function testWildcardQueueWorkerWorksAllQueues()
    {
        $worker = new Resque_Worker('*');
        $worker->registerWorker();

        Resque::enqueue('queue1', 'Test_Job_1');
        Resque::enqueue('queue2', 'Test_Job_2');

        $job = $worker->reserve();
        $this->assertEquals('queue1', $job->queue);

        $job = $worker->reserve();
        $this->assertEquals('queue2', $job->queue);
    }

    public function testWorkerDoesNotWorkOnUnknownQueues()
    {
        $worker = new Resque_Worker('queue1');
        $worker->registerWorker();
        Resque::enqueue('queue2', 'Test_Job');

        $this->assertFalse($worker->reserve());
    }

    public function testWorkerClearsItsStatusWhenNotWorking()
    {
        Resque::enqueue('jobs', 'Test_Job');
        $worker = new Resque_Worker('jobs');
        $job = $worker->reserve();
        $worker->workingOn($job);
        $worker->doneWorking();
        $this->assertEquals(array(), $worker->job());
    }

    public function testWorkerRecordsWhatItIsWorkingOn()
    {
        $worker = new Resque_Worker('jobs');
        $worker->registerWorker();

        $payload = array(
            'class' => 'Test_Job'
        );
        $job = new Resque_Job('jobs', $payload);
        $worker->workingOn($job);

        $job = $worker->job();
        $this->assertEquals('jobs', $job['queue']);
        if(!isset($job['run_at'])) {
            $this->fail('Job does not have run_at time');
        }
        $this->assertEquals($payload, $job['payload']);
    }

    public function testWorkerErasesItsStatsWhenShutdown()
    {
        Resque::enqueue('jobs', 'Test_Job');
        Resque::enqueue('jobs', 'Invalid_Job');

        $worker = new Resque_Worker('jobs');
        $worker->work(0);
        $worker->work(0);

        $this->assertEquals(0, $worker->getStat(Resque::PROCESSED));
        $this->assertEquals(0, $worker->getStat(Resque::FAILED));
    }

    public function testWorkerCleansUpDeadWorkersOnStartup()
    {
        // Register a good worker
        $goodWorker = new Resque_Worker('jobs');
        $goodWorker->registerWorker();
        $workerId = explode(':', $goodWorker);
        $goodWorker->registerLogger(new MonologInit('', ''));

        // Register some bad workers
        $worker = new Resque_Worker('jobs');
        $worker->setId($workerId[0].':1:jobs');
        $worker->registerWorker();
        $worker->registerLogger(new MonologInit('', ''));

        $worker = new Resque_Worker(array('high', 'low'));
        $worker->setId($workerId[0].':2:high,low');
        $worker->registerWorker();
        $worker->registerLogger(new MonologInit('', ''));

        $this->assertEquals(3, count(Resque_Worker::all()));

        $goodWorker->pruneDeadWorkers();

        // There should only be $goodWorker left now
        $this->assertEquals(1, count(Resque_Worker::all()));
    }

    public function testDeadWorkerCleanUpDoesNotCleanUnknownWorkers()
    {
        // Register a bad worker on this machine
        $worker = new Resque_Worker('jobs');
        $workerId = explode(':', $worker);
        $worker->setId($workerId[0].':1:jobs');
        $worker->registerWorker();
        $worker->registerLogger(new MonologInit('', ''));

        // Register some other false workers
        $worker = new Resque_Worker('jobs');
        $worker->setId('my.other.host:1:jobs');
        $worker->registerWorker();
        $worker->registerLogger(new MonologInit('', ''));

        $this->assertEquals(2, count(Resque_Worker::all()));

        $worker->pruneDeadWorkers();

        // my.other.host should be left
        $workers = Resque_Worker::all();
        $this->assertEquals(1, count($workers));
        $this->assertEquals((string)$worker, (string)$workers[0]);
    }

    public function testWorkerFailsUncompletedJobsOnExit()
    {
        $worker = new Resque_Worker('jobs');
        $worker->registerWorker();

        $payload = array(
            'class' => 'Test_Job',
            'id' => 'randomId'
        );
        $job = new Resque_Job('jobs', $payload);

        $worker->workingOn($job);
        $worker->unregisterWorker();

        $this->assertEquals(1, Resque_Stat::get(Resque::FAILED));
    }

    public function testWorkerLogAllMessageOnVerbose()
    {
        $worker = new Resque_Worker('jobs');
        $worker->logLevel = Resque_Worker::LOG_VERBOSE;
        $worker->logOutput = fopen('php://memory', 'r+');

        $message = array('message' => 'x', 'data' => []);

        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_DEBUG));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_INFO));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_WARNING));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_CRITICAL));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_ERROR));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_ALERT));

        rewind($worker->logOutput);
        $output = stream_get_contents($worker->logOutput);

        $lines = explode("\n", $output);
        $this->assertEquals(6, count($lines) -1);
    }

    public function testWorkerLogOnlyInfoMessageOnNonVerbose()
    {
        $worker = new Resque_Worker('jobs');
        $worker->logLevel = Resque_Worker::LOG_NORMAL;
        $worker->logOutput = fopen('php://memory', 'r+');

        $message = array('message' => 'x', 'data' => []);

        $this->assertEquals(false, $worker->log($message, Resque_Worker::LOG_TYPE_DEBUG));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_INFO));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_WARNING));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_CRITICAL));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_ERROR));
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_ALERT));

        rewind($worker->logOutput);
        $output = stream_get_contents($worker->logOutput);

        $lines = explode("\n", $output);
        $this->assertEquals(5, count($lines) -1);
    }

    public function testWorkerLogNothingWhenLogNone()
    {
        $worker = new Resque_Worker('jobs');
        $worker->logLevel = Resque_Worker::LOG_NONE;
        $worker->logOutput = fopen('php://memory', 'r+');

        $message = array('message' => 'x', 'data' => '');

        $this->assertEquals(false, $worker->log($message, Resque_Worker::LOG_TYPE_DEBUG));
        $this->assertEquals(false, $worker->log($message, Resque_Worker::LOG_TYPE_INFO));
        $this->assertEquals(false, $worker->log($message, Resque_Worker::LOG_TYPE_WARNING));
        $this->assertEquals(false, $worker->log($message, Resque_Worker::LOG_TYPE_CRITICAL));
        $this->assertEquals(false, $worker->log($message, Resque_Worker::LOG_TYPE_ERROR));
        $this->assertEquals(false, $worker->log($message, Resque_Worker::LOG_TYPE_ALERT));

        rewind($worker->logOutput);
        $output = stream_get_contents($worker->logOutput);

        $lines = explode("\n", $output);
        $this->assertEquals(0, count($lines) -1);
    }

    public function testWorkerLogWithISOTime()
    {
        $worker = new Resque_Worker('jobs');
        $worker->logLevel = Resque_Worker::LOG_NORMAL;
        $worker->logOutput = fopen('php://memory', 'r+');

        $message = array('message' => 'x', 'data' => []);

        $now = date('c');
        $this->assertEquals(true, $worker->log($message, Resque_Worker::LOG_TYPE_INFO));

        rewind($worker->logOutput);
        $output = stream_get_contents($worker->logOutput);

        $lines = explode("\n", $output);
        $this->assertEquals(1, count($lines) -1);
        $this->assertEquals('[' . $now . '] x', $lines[0]);
    }

    public function testWorkersCleanupWithPrefix() {
        $payload = array(
            'class' => 'Test_Job'
        );

        $worker1 = $this->createWorker("prod-worker-12345:1:jobs", $payload);
        $worker2 = $this->createWorker("prod-worker-12346:1:jobs", $payload);
        $worker3 = $this->createWorker("prod-worker-12347:1:jobs", $payload);
        $worker4 = $this->createWorker("prod-rc-worker-12346:1:jobs", $payload);
        $worker5 = $this->createWorker("prod-rc-worker-12347:1:jobs", $payload);
        $worker6 = $this->createWorker("prod-worker-scheduler-12347:1:jobs", $payload);

        Resque::redis()->del(Resque::WORKER_PREFIX . $worker1 . Resque::PING_SUFFIX);
        Resque::redis()->del(Resque::WORKER_PREFIX . $worker3 . Resque::PING_SUFFIX);
        Resque::redis()->del(Resque::WORKER_PREFIX . $worker5 . Resque::PING_SUFFIX);
        Resque::redis()->del(Resque::WORKER_PREFIX . $worker6 . Resque::PING_SUFFIX);

        $this->assertCount(2, Resque::cleanWorkers('prod-worker-'));
        $this->assertCount(4, Resque_Worker::all());
    }

    public function testWorkersCleanupNoPrefix() {
        $payload = array(
            'class' => 'Test_Job'
        );

        $worker1 = $this->createWorker("prod-worker-12345:1:jobs", $payload);
        $worker2 = $this->createWorker("prod-worker-12346:1:jobs", $payload);
        $worker3 = $this->createWorker("prod-worker-12347:1:jobs", $payload);
        $worker4 = $this->createWorker("prod-rc-worker-12346:1:jobs", $payload);
        $worker5 = $this->createWorker("prod-rc-worker-12347:1:jobs", $payload);
        $worker6 = $this->createWorker("prod-worker-scheduler-12347:1:jobs", $payload);

        Resque::redis()->del(Resque::WORKER_PREFIX . $worker1 . Resque::PING_SUFFIX);
        Resque::redis()->del(Resque::WORKER_PREFIX . $worker3 . Resque::PING_SUFFIX);
        Resque::redis()->del(Resque::WORKER_PREFIX . $worker5 . Resque::PING_SUFFIX);
        Resque::redis()->del(Resque::WORKER_PREFIX . $worker6 . Resque::PING_SUFFIX);

        $this->assertCount(3, Resque::cleanWorkers());
        $this->assertCount(3, Resque_Worker::all());
    }

    public function testGetInProgressJobsCountWithPrefix() {
        $payload = array(
            'id' => 'id',
            'class' => 'Test_Job'
        );

        $this->assertEquals(0, Resque::getInProgressJobsCount('prod-worker-'));

        $worker1 = $this->createWorker("prod-worker-12345:1:jobs", $payload);
        $worker2 = $this->createWorker("prod-worker-12346:1:jobs", $payload);
        $worker3 = $this->createWorker("prod-worker-12347:1:jobs", $payload);
        $worker4 = $this->createWorker("prod-rc-worker-12346:1:jobs", $payload);
        $worker5 = $this->createWorker("prod-rc-worker-12347:1:jobs", $payload);

        $this->assertEquals(3, Resque::getInProgressJobsCount('prod-worker-'));

        $worker1->unregisterWorker();
        $worker2->unregisterWorker();
        $worker3->unregisterWorker();

        $this->assertEquals(0, Resque::getInProgressJobsCount('prod-worker-'));
    }

    public function testGetInProgressJobsCountWithoutPrefix() {
        $payload = array(
            'class' => 'Test_Job'
        );

        $this->assertEquals(0, Resque::getInProgressJobsCount());

        $worker1 = $this->createWorker("prod-worker-12345:1:jobs", $payload);
        $worker2 = $this->createWorker("prod-worker-12346:1:jobs", $payload);
        $worker3 = $this->createWorker("prod-worker-12347:1:jobs", $payload);
        $worker4 = $this->createWorker("prod-rc-worker-12346:1:jobs", $payload);
        $worker5 = $this->createWorker("prod-rc-worker-12347:1:jobs", $payload);

        $this->assertEquals(5, Resque::getInProgressJobsCount());

        Resque::redis()->del(Resque::CURRENT_JOBS);

        $this->assertEquals(0, Resque::getInProgressJobsCount());
    }

    public function testGetJobsToRerunWithPrefix() {
        $payload1 = array(
            'class' => 'Test_Job1',
            'args' => []
        );

        $payload2 = array(
            'class' => 'Test_Job2',
            'args' => []
        );

        $payload3 = array(
            'class' => 'Test_Job1',
            'args' => ['rerun' => true]
        );

        $worker1 = $this->createWorker("prod-worker-12345:1:jobs", $payload1);
        $worker2 = $this->createWorker("prod-worker-12346:1:jobs", $payload3);
        $worker3 = $this->createWorker("prod-worker-12347:1:jobs", $payload2);
        $worker4 = $this->createWorker("prod-rc-worker-12346:1:jobs", $payload1);
        $worker5 = $this->createWorker("prod-rc-worker-12347:1:jobs", $payload2);
        sleep(3);
        $worker6 = $this->createWorker("prod-worker-12348:1:jobs", $payload1);
        sleep(2);

        $this->assertCount(1, Resque::getJobsToRerun('prod-worker-', 4, ['Test_Job1']));
        $this->assertEquals(3, Resque::getInProgressJobsCount('prod-worker-'));
    }

    public function testGetJobsToRerunWithoutPrefix() {
        $payload1 = array(
            'class' => 'Test_Job1',
            'args' => []
        );

        $payload2 = array(
            'class' => 'Test_Job2',
            'args' => []
        );

        $payload3 = array(
            'class' => 'Test_Job1',
            'args' => ['rerun' => true]
        );

        $worker1 = $this->createWorker("prod-worker-12345:1:jobs", $payload1);
        $worker2 = $this->createWorker("prod-worker-12346:1:jobs", $payload3);
        $worker3 = $this->createWorker("prod-worker-12347:1:jobs", $payload2);
        $worker4 = $this->createWorker("prod-rc-worker-12346:1:jobs", $payload1);
        $worker5 = $this->createWorker("prod-rc-worker-12347:1:jobs", $payload2);
        sleep(3);
        $worker6 = $this->createWorker("prod-worker-12348:1:jobs", $payload1);
        sleep(2);

        $this->assertCount(2, Resque::getJobsToRerun(null, 4, ['Test_Job1']));
        $this->assertEquals(4, Resque::getInProgressJobsCount());
    }

    private function createWorker($workerId, $payload): Resque_Worker {
        $worker = new Resque_Worker('jobs');
        $worker->setId($workerId);
        $worker->registerWorker();
        $worker->registerLogger(new MonologInit('', ''));
        $job = new Resque_Job('jobs', $payload);
        $worker->workingOn($job);
        return $worker;
    }
}
