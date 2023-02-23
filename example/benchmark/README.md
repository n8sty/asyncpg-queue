# Performance benchmark

The performance benchmark is meant to, for a given network and server setup, provide
an estimate of the maximum put and pop throughout that's possible. Remember, these
values will not be particularly high because of the reliance on Postgres, but they are
likely high enough for many simpler use cases.

Start a Docker container for the queue:

    docker run \
      --rm
      --name asyncpg-queue-benchmark \
      -d \
      -p 5433 \
      -e POSTGRES_HOST_AUTH_METHOD=trust
      postgres:15-alpine

Run the producer script to get an estimate of enqueuing throughput:

    python example/benchmark/producer.py 1000  # tasks to enqueue
    # Enqueued 1000 tasks in 1.488s (0.0015s/task)

Run the consumer script to get an estimate of dequeuing and processing a no-op task:

    python example/benchmark/consumer.py 10  # number of concurrent coroutines
    # Processed 1000 tasks in 56.29s (0.056s/task)
