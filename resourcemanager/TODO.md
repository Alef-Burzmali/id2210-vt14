# Sparrow

## Article exceprts

### Per-task sampling

The scheduler randomly selects two worker machines for each task and sends a probe to each, 
where a probe is a lightweight RPC. The worker machines each reply to the probe with the number 
of currently queued tasks, and the scheduler places the task on the worker machine with the shortest queue.

The scheduler repeats this process for each task in the job
			
### Batch sampling

Batch sampling aggregates load information from the probes sent for all of a job’s tasks, and places the job’s 
m tasks on the least loaded of all the worker machines probed.

To schedule using batch sampling, a scheduler ran- domly selects dm worker machines (for d ≥ 1). 
The scheduler sends a probe to each of the dm workers; as with per-task sampling, 
each worker replies with the number of queued tasks. The scheduler places 
one of the job’s m tasks on each of the m least loaded workers.

### Late binding

With late binding, workers do not re- ply immediately to probes and instead place a reservation for the task 
at the end of an internal work queue. When this reservation reaches the front of the queue, the worker sends an 
RPC to the scheduler that initiated the probe requesting a task for the corresponding job. The scheduler assigns 
the job’s tasks to the first m workers to reply, and replies to the remaining (d − 1)m workers with a no-op signaling 
that all of the job’s tasks have been launched.

### Proactive cancellation

proactively send a cancellation RPC to all workers with outstanding probes

## TODO

### `handleRequestResource`

- Pick up 4 random neighbours
- Send one `probe` to each of them
- Upon reception of the four probes, send a request resource to the least loaded worker
- Upon reception of the request response, start the timeout
- Upon timeout termination, release the allocation

