# Metadata ETL Pipeline Training

A training-only reconstruction of a two-phase, fan-out serverless ETL
pipeline: **ingest** raw batched data into a store, then **transform** it
into a normalized output shape, entirely via S3-coordinated, independently
invokable AWS Lambda functions. It is modeled on a real production pipeline
but every proprietary calculation, resource ID, and network detail has been
replaced with fabricated, generic equivalents (see [What's fabricated vs
real](#whats-fabricated-vs-real) below).

## Why this shape?

Lambda functions are stateless and short-lived, so a job that needs many
files processed in parallel, followed by "did everything finish?"
coordination, can't just hold everything in one process's memory across
invocations. This pipeline solves that with two complementary patterns:

1. **Fan-out via direct Lambda invocation.** A "dispatch" Lambda discovers
   how many independent units of work exist (batches, or files within a
   batch) and invokes a "worker" Lambda once per unit, in parallel, instead
   of pushing N messages onto a queue. This keeps 1 dispatch : N workers
   mapping explicit and lets the dispatcher collect every worker's result
   before deciding what happens next.
2. **Sentinel files for cross-invocation signaling.** Since workers can't
   share memory, "is stage A finished yet?" is answered by polling for a
   marker object in S3 (`app/sentinel.py`) rather than blocking on a
   future or holding a long-lived connection open.

## Two phases

### Phase 1: Ingest

```
Step Functions (waitForTaskToken)
        |  sends 1 SQS message {jobId, taskToken, bucket, prefix}
        v
ingest_dispatch_handler (SQS-triggered)
        |  lists batch sub-prefixes under the job prefix
        |  fans out: invokes ingest_worker_handler once per batch
        v
ingest_worker_handler (direct Lambda invoke, x N batches)
        |  polls for this batch's completion sentinel
        |  loads the batch's rows once the sentinel appears
        v
ingest_dispatch_handler aggregates all N results
        |  publishes an SNS message containing the original task token
        v
Step Functions execution resumes (COMPLETE or ERROR)
```

The Step Functions `waitForTaskToken` service-integration pattern lets a
state machine send a message and then *pause* -- holding a token -- until
something calls back with that same token. Here, the callback is an SNS
publish once every fanned-out worker has reported in
(`app/callback.py::build_callback_message`).

### Phase 2: Transform

```
S3 ObjectCreated event (context file: *_CONTEXT_OUTPUT.json)
        v
transform_dispatch_handler (S3-event-triggered)
        |  reads the context file for job metadata
        |  lists every sibling file in the batch
        |  fans out: invokes transform_worker_handler once per file
        v
transform_worker_handler (direct Lambda invoke, x N files)
        |  reads the file's S3 object metadata tags: record-type / record-subtype
        |  routes to the transform registered for that pair (app/router.py)
        |  writes a per-file .out result
        v
transform_dispatch_handler merges every .out file into one merged.json
        |  writes the batch's completion sentinel
```

Unlike phase 1, there's no queue in front of the transform dispatcher --
the S3 event invokes it directly. This is a deliberate architectural
contrast worth noticing: phase 1 needs a queue because it's kicked off by
Step Functions (which can only start executions or send messages, not
invoke Lambdas directly as a state), while phase 2 is naturally
event-driven from an S3 upload.

## Two intentional improvements over the system this is modeled on

The original production pipeline this trains on works reliably, but two of
its internals were simplified/hardened here as explicit teaching points:

1. **Registry-based transform dispatch (`app/router.py`) instead of a long
   if/elif chain.** The original routes to one of ~25 domain transform
   functions via a chain of `if record_type == "X": ... elif record_type ==
   "Y" and record_subtype == "Z": ...`. That's easy to grow into an
   unmaintainable wall of conditionals, and nothing stops two branches from
   silently overlapping. This version has each transform module
   self-register via a `@register(record_type, record_subtype)` decorator
   against a dict keyed on that pair; registering the same pair twice
   raises `DuplicateTransformError` at import time instead of silently
   letting the first matching branch win.
2. **`ThreadPoolExecutor` fan-out (`app/fanout.py`) instead of
   `multiprocessing.Process` + `Manager().dict()`.** Invoking another
   Lambda is I/O-bound waiting, not CPU-bound work, so a thread pool is
   sufficient -- and it's dramatically easier to unit test deterministically,
   since there's no process pickling or manager-process lifecycle to fake.

## Layout

```
app/
  aws_clients.py          # Protocol interfaces: S3Client, LambdaInvoker, SnsPublisher, DataLoader
  sentinel.py             # write/poll for completion sentinels in S3
  fanout.py               # ThreadPoolExecutor-based parallel Lambda invocation
  callback.py             # build/publish the Step Functions waitForTaskToken callback
  s3_prefixes.py          # discover batch prefixes / sibling files
  router.py               # registry-based (record_type, record_subtype) -> transform dispatch
  merge.py                # concatenate per-file transform outputs into one batch output
  transforms/              # fabricated generic "telemetry batch" transforms (see below)
  handlers/
    ingest_dispatch_handler.py     # SQS-triggered: fan out ingest workers, publish callback
    ingest_worker_handler.py       # poll sentinel, then "load" a batch
    transform_dispatch_handler.py  # S3-event-triggered: fan out transform workers, merge, sentinel
    transform_worker_handler.py    # read tags, dispatch to a transform, write .out
tests/
  fakes.py                 # in-memory FakeS3Client / FakeLambdaInvoker / FakeSnsPublisher / FakeDataLoader
  test_*.py                 # 34 tests covering every module above, no real AWS access needed
infra/cdk/                 # TypeScript CDK: SQS+DLQ, SNS, 4 Lambdas, S3 event notification,
                            # and a Step Functions state machine using waitForTaskToken
```

## What's fabricated vs. real

This training is modeled on a real ETS pipeline that ingests and transforms
psychometric statistics data. Per an explicit "no calculations" constraint
for this training series, **no real domain logic, column names, resource
IDs, network ranges, or internal hostnames are reproduced anywhere here**.
Instead:

- The domain is a fictional **telemetry batch** (sensor readings), not
  statistics -- see `app/transforms/`: `transform_wide_to_long.py`,
  `transform_summary.py` (count/min/max/mean per sensor),
  `transform_counts.py` (category counts), `transform_flags.py` (threshold
  flagging).
- `app/handlers/ingest_worker_handler.py`'s "load a batch" step is a
  trivial row-counting reference implementation (`DataLoader` protocol);
  a real deployment would bulk-load into a database, which is out of
  scope here.
- All CDK resource names, the fictional VPC CIDR ranges in
  `infra/cdk/lib/constants.ts`, and account/region values are made up for
  this repo and don't correspond to anything real.

## Running it

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest -q          # 34 tests, no AWS access needed (everything is dependency-injected)
flake8 app tests
mypy
```

### CDK (infrastructure)

```bash
cd infra/cdk
npm install
npx cdk synth       # verified to synthesize cleanly in this training's CI
```

## Exercises

1. **Add a third phase.** Sketch (and optionally implement) a "notify"
   phase that fans out an email/webhook per recipient once `merged.json`
   is written, reusing `app/fanout.py`.
2. **Make `has_errors` partial-failure-aware.** Currently any single
   worker error flips the whole batch to `ERROR`. Change
   `app/callback.py`/`transform_dispatch_handler.py` so a configurable
   failure threshold (e.g. "allow up to 10% of files to fail") still
   reports `COMPLETE`, with the failed items listed separately.
3. **Add a transform without touching the router's internals.** Register
   a new `(record_type, record_subtype)` pair in `app/transforms/` and
   confirm `app/router.py` raises `DuplicateTransformError` if you
   accidentally reuse an existing pair.
4. **Convert the SQS dispatch to EventBridge.** Replace the
   `tasks.SqsSendMessage` + `SqsEventSource` pair in
   `infra/cdk/lib/stack.ts` with an EventBridge rule invoking the ingest
   dispatcher directly, and discuss what changes about retry/DLQ
   semantics when you do.
5. **Bound the fan-out concurrency under load.** `app/fanout.py`'s
   `max_workers` is a fixed default. Add a test proving that a batch of
   500 items never issues more than `max_workers` concurrent invocations.
