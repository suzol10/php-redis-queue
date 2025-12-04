<?php

namespace PhpRedisQueue\managers;

use PhpRedisQueue\models\Queue;

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
    $this->migrateQueues();

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
    foreach (['pending', 'processed', 'failed'] as $which) {

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
  protected function migrateQueues()
  {
    // Find all queue keys that might have the old failed counter format
    $queueKeys = $this->redis->keys('php-redis-queue:client:*:processed');

    foreach ($queueKeys as $processedKey) {
      // Extract queue name from key: php-redis-queue:client:{queue_name}:processed
      if (preg_match('/php-redis-queue:client:([^:]+):processed/', $processedKey, $matches)) {
        $queueName = $matches[1];
        $queue = $this->getQueue($queueName);

        // Check if failed key contains a counter (string/int) instead of a list
        try {
          $failedValue = $this->redis->get($queue->failed) ?: 0;
        } catch (\Exception $e) {
          $failedValue = null;
        }

        if ($failedValue !== null && is_numeric($failedValue)) {
          // This is an old counter format, convert to list
          // Since we don't have the actual job IDs, we'll initialize as empty list
          // and set successful counter if it doesn't exist
          $this->redis->del($queue->failed); // Remove the old counter failed counter
        }
        
        // Ensure successful counter exists
        if (!$this->redis->exists($queue->successful)) {
          $this->redis->set($queue->successful, 0);
        }
        
      }
    }
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
          $this->redis->lpush($queue->pending, $jobId);

          try {
            // Update job status back to pending
            $job = new \PhpRedisQueue\models\Job($this->redis, (int) $jobId);
            if ($job->get() !== null) {
              $job->withData('status', 'pending')->save();
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
}
