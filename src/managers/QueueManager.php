<?php

namespace PhpRedisQueue\managers;

use Doctrine\DBAL\Driver\Mysqli\Exception\FailedReadingStreamOffset;
use PhpRedisQueue\models\Queue;
use PHPUnit\TextUI\XmlConfiguration\MigrationBuilderException;

class QueueManager extends BaseManager
{
  const GLOBAL_WORKERS_LIMIT_KEY = 'php-redis-queue:global:workers_limit';
  const DEFAULT_GLOBAL_WORKERS_LIMIT = 10;
  /**
   * Redis key that holds the hash that keeps track
   * of active queues.
   * @var string
   */
  protected string $allQueues = 'php-redis-queue:queues';

  public function __construct(\Predis\Client $redis)
  {
    parent::__construct($redis);
    if (!$this->redis->exists(self::GLOBAL_WORKERS_LIMIT_KEY) ) {
      $this->setGlobalWorkersLimit(self::DEFAULT_GLOBAL_WORKERS_LIMIT);
    }
  }

  public function setGlobalWorkersLimit(int $globalWorkersLimit)
  {
    $this->redis->set(self::GLOBAL_WORKERS_LIMIT_KEY, $globalWorkersLimit);
  }

  public function getGlobalWorkersLimit(): ?int
  {
    return $this->redis->exists(self::GLOBAL_WORKERS_LIMIT_KEY) ? (int) $this->redis->get(self::GLOBAL_WORKERS_LIMIT_KEY) : null;
  }

  /**
   * Get queues with an active worker
   * @return array
   */
  public function getActiveQueues()
  {
    return $this->redis->hgetall($this->allQueues);
  }

  /**
   * Get total active workers across all queues
   * @return int
   */
  public function getTotalActiveWorkers(): int
  {
    $stats = $this->getList();
    $totalActiveWorkers = 0;
    foreach ($stats as $queueName => $queueStats) {
      $totalActiveWorkers += $queueStats['count'] ?? 0;
    }

    return $totalActiveWorkers;
  }

  /**
   * Get a list of all (active or not) queues, how many workers are available
   * per queue, and number of pending and processed jobs.
   * @return array
   */
  public function getList()
  {
    $this->verifyQueues();

    // active queues (with or without pending jobs)
    $queues = [];
    $activeQueues = $this->getActiveQueues();

    foreach ($activeQueues as $queueName) {
      if (!isset($queues[$queueName])) {
        $queues[$queueName] = [
          'name' => $queueName,
          'count' => 0,
          'pending' => 0,
          'processed' => 0,
          'successful' => 0,
          'failed' => 0,
        ];
      }

      $queues[$queueName]['count']++;
    }

    // get jobs on all pending queues (active queues or not)
    $queues = $this->addJobsFromQueue($queues);

    return $queues;
  }

  protected function addJobsFromQueue(array $queues)
  {
    foreach (['pending', 'successful', 'failed'] as $which) {

      $foundQueues = $this->redis->keys("php-redis-queue:client:*:$which");

      foreach ($foundQueues as $keyName) {
        preg_match("/php-redis-queue:client:([^:]+):$which/", $keyName, $match);

        if (!isset($match[1])) {
          continue;
        }

        $queueName = $match[1];

        if (!isset($queues[$queueName])) {
          $queues[$queueName] = [
            'name' => $queueName,
            'count' => 0,
            'pending' => 0,
            'processed' => 0,
            'successful' => 0,
            'failed' => 0,
          ];
        }

        $queues[$queueName] = array_merge($queues[$queueName], $this->getQueueStats($queueName));
      }
    }


    return $queues;
  }

  /**
   * Get a job by ID
   * @return array
   */
  public function getQueue(string $name)
  {
    return (new Queue($this->redis, $name));
  }

  /**
   * Get detailed statistics for a specific queue
   * @param string $name Queue name
   * @return array
   */
  public function getQueueStats(string $name): array
  {
    $queue = $this->getQueue($name);
    /** @var \PhpRedisQueue\models\Queue $queue */
    $stats = $queue->getStats();

    // Add queue name to the stats
    $stats['name'] = $name;

    return $stats;
  }

  public function registerQueue(Queue $queue)
  {
    // register this queue
    $this->redis->hset($this->allQueues, $this->redis->client('id'), $queue->name);

    // verify that registered queues are still running. if they aren't, remove them.
    $this->verifyQueues();
  }

  /**
   * There isn't a reliable way to tell when a blocking queue worker
   * has stopped running (PHP can't detect SIGINT or SIGTERM of a blocking
   * worker), so let's instead verify the existing queues each time
   * a new queue worker is instatntiated.
   * @return void
   */
  protected function verifyQueues()
  {
    // get an array of active client IDs as the keys
    $clients = $this->redis->client('list');
    $clientIds = array_flip(array_map(fn ($client) => $client['id'], $clients));

    // get the hash of registered queues
    $queues = $this->redis->hgetall($this->allQueues);

    // figure out which queues are inactive
    $inactive = array_keys(array_diff_key($queues, $clientIds));

    // if there are inactives, remove them
    if (!empty($inactive)) {
      $this->redis->hdel($this->allQueues, $inactive);
    }
  }

  /**
   * Migrate existing queues to use the new failed jobs list format
   * @return void
   */
  public function migrateQueues()
  {
    // NOTE: processed was a list, we want to rename it to successful, and successful to processed. this will make successful a list and processed a number.
    // NOTE: since this is a migration, we need to make sure we only run it when necessary.
    // NOTE: we can't use internal functions because they are already on the new format. we need to use redis commands directly.
    // steps:
    // 1. get all queue keys that have processed key and check if successful key is not a list and processed key is a list
    // 2. if so, rename processed to successful_temp
    // 3. rename successful to processed
    // 4. rename successful_temp to successful

    $processedKeys = $this->redis->keys('php-redis-queue:client:*:processed');

    $migratedCount = 0;
    foreach ($processedKeys as $processedKey) {
      preg_match("/php-redis-queue:client:([^:]+):processed/", $processedKey, $match);
      if (!isset($match[1])) {
        continue;
      }
      $queueName = $match[1];
      $queue = $this->getQueue($queueName);
      /** @var \PhpRedisQueue\models\Queue $queue */
      if ($this->redis->exists($queue->successful) && $this->redis->exists($queue->processed)) {
        // check if processed key is a list and successful key is a counter
        if ($this->redis->type($queue->successful) == 'list') {
          // already migrated
          continue;
        }

        $this->redis->rename($queue->processed, $queue->successful . '_temp');
        $this->redis->rename($queue->successful, $queue->processed);
        $this->redis->rename($queue->successful . '_temp', $queue->successful);
        $migratedCount++;
      }
    }

    return $migratedCount;
  }

  /**
   * Recover jobs that are stuck in processing queues from crashed workers.
   * This should only be called once during worker system initialization.
   * @return int Number of jobs recovered
   */
  public function recoverCrashedJobs(): int
  {
    // Get all queues with their stats
    $queues = $this->getList();
    $recoveredCount = 0;

    foreach ($queues as $queueName => $queueStats) {
      // Only process queues that have jobs stuck in processing
      if ($queueStats['processing'] > 0) {
        $queue = $this->getQueue($queueName);

        // Move all jobs from processing back to pending
        while ($jobId = $this->redis->rpop($queue->processing)) {
          try {
            // Update job status back to pending
            $job = new \PhpRedisQueue\models\Job($this->redis, (int) $jobId);
            if ($job->get() !== null) {
              $job->withData('status', 'pending')->save();
              $this->redis->lpush($queue->pending, $jobId);
              $recoveredCount++;
            } else {
              // Job data doesn't exist, skip it
              $this->log('warning', 'Skipping recovery of job ' . $jobId . ': job data not found');
            }
          } catch (\Exception $e) {
            // Skip jobs that can't be loaded or updated
            $this->log('warning', 'Failed to recover job ' . $jobId . ': ' . $e->getMessage());
          }
        }
      }
    }

    return $recoveredCount;
  }

  public function fixIncorrectFailedJobsStatus(): int
  {
    // Get all queues with their stats
    $queues = $this->getList();
    $recoveredCount = 0;

    foreach ($queues as $queueName => $queueStats) {
      // Only process queues that have jobs stuck in processing
      if ($queueStats['failed'] > 0) {
        /** @var \PhpRedisQueue\models\Queue $queue */
        $queue = $this->getQueue($queueName);

          // get default number of jobs to fix
          $failedJobs = $queue->getJobs('failed' , -1);
          foreach ($failedJobs as $jobStdObj) {

            /** @var \PhpRedisQueue\models\Job $Job */
            $job = new \PhpRedisQueue\models\Job($this->redis, (int) $jobStdObj->id);

            // if job is not in failed status, update it to failed
            if ($job->get()['status'] !== 'failed') {
              $job->withData('status', 'failed')->save();
              $recoveredCount++;
            }  
          }
      }
    }

    return $recoveredCount;
  }
}
