<?php

namespace PhpRedisQueue\models;

class Queue
{
  /**
   * Queue name
   * @var string
   */
  public string $name;

  /**
   * Name of the list that contains jobs waiting to be worked on
   * @var string
   */
  public string $pending;

  /**
   * Name of list that contains jobs currently being worked on
   * @var string
   */
  public string $processing;

  /**
   * Name of varible that keeps track of how many jobs have processed in this queue
   * @var string
   */
  public string $processed;


  /**
   * Name of varible that keeps track of how many jobs have processed successfully in this queue
   * @var string
   */
  public string $successful;

  /**
   * Name of list that keeps track of failed jobs in this queue
   * @var string
   */
  public string $failed;

  public function __construct(protected \Predis\Client $redis, string $name)
  {
    $this->name = str_replace(':', '-', $name);

    $base = 'php-redis-queue:client:' . $this->name;

    $this->pending = $base . ':pending';
    $this->processing = $base . ':processing';
    $this->processed = $base . ':processed';
    $this->successful = $base . ':successful';
    $this->failed = $base . ':failed';
  }

  public function getJobs(string $which, int $limit = 50)
  {
    $jobs = $this->redis->lrange($this->$which, 0, $limit);

    return array_map(function ($jobId) {
      return json_decode($this->redis->get('php-redis-queue:jobs:'. $jobId));
    }, $jobs);
  }

  /**
   * Check the queue for jobs
   * @param bool $block       TRUE to use blpop(); FALSE to use lpop()
   * @return array|string|null
   */
  public function check(bool $block = true)
  {
    if ($block) {
      return $this->redis->blpop($this->pending, 0);
    }

    return $this->redis->lpop($this->pending);
  }

  /**
   * Remove a job from the processing queue
   * @param array $job Job data
   * @return int
   */
  public function removeFromProcessing(Job $job): int
  {
    return $this->redis->lrem($this->processing, -1, $job->id());
  }

  /**
   * Move completed job to processed queue (success or fail)
   * @return int
   */
  public function onJobCompletion(Job $job)
  {
    $this->removeFromProcessing($job);

    $status = $job->get('status');
    if ($status === 'success') {
        $this->redis->incr($this->successful);
        $this->redis->lpush($this->processed, $job->id());
    } elseif ($status === 'failed') {
        $this->redis->lpush($this->failed, $job->id());
    }

    return true;
  }

  public function getStats(): array
  {
      return [
          'pending' => $this->redis->llen($this->pending),
          'processing' => $this->redis->llen($this->processing),
          'processed' => $this->redis->llen($this->processed),
          'successful' => (int) $this->redis->get($this->successful) ?: 0,
          'failed' => $this->redis->llen($this->failed),
          'config' => [
              'max_workers' => $this->getConfigDataValue('max_workers') ?: 1,
              'pending_threshold' => $this->getConfigDataValue('pending_threshold') ?: 5,
          ],
      ];
  }

  /**
   * Get failed job IDs from the queue
   * @param int $limit Maximum number of failed jobs to return
   * @return array Array of failed job IDs
   */
  public function getFailedJobs(int $limit = 50): array
  {
    return $this->redis->lrange($this->failed, 0, $limit - 1);
  }

  /**
   * Get configuration data value for this queue
   * @param string $key Configuration key
   * @return mixed Configuration value or null if not set
   */
  public function getConfigDataValue(string $key)
  {
    $configKey = $this->getConfigDataKey($key);
    return $this->redis->get($configKey);
  }

  /**
   * Set configuration data value for this queue
   * @param string $key Configuration key
   * @param mixed $value Configuration value
   * @return bool True on success
   */
  public function setConfigDataValue(string $key, $value): bool
  {
    $configKey = $this->getConfigDataKey($key);
    $status = (string) $this->redis->set($configKey, $value);
    return $status === 'OK' || $status === 'QUEUED';
  }

  /**
   * Get the Redis key for configuration data
   * @param string $key Configuration key
   * @return string Redis key for the config data
   */
  private function getConfigDataKey(string $key): string
  {
    return 'php-redis-queue:client:' . $this->name . ':config:' . $key;
  }
}
