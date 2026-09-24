import * as path from "path";
import { Duration, RemovalPolicy, Stack, StackProps } from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import * as events from "aws-cdk-lib/aws-lambda-event-sources";
import { Construct } from "constructs";
import { CONTEXT_FILE_SUFFIX, EnvConfig } from "./constants";

export interface MetadataEtlPipelineStackProps extends StackProps {
  readonly config: EnvConfig;
}

/**
 * Two-phase fan-out ETL pipeline, training edition.
 *
 * Phase 1 (ingest): a Step Functions state machine sends one SQS message per
 * job (using the `waitForTaskToken` service-integration pattern, so the state
 * machine execution pauses until the pipeline reports back). The ingest
 * dispatch Lambda (SQS-triggered) discovers per-batch prefixes under the
 * job's S3 prefix and directly invokes the ingest worker Lambda once per
 * batch. Each worker waits for a batch-level completion sentinel, then loads
 * its rows. The dispatcher aggregates worker results and publishes an SNS
 * message containing the original task token, resuming the state machine.
 *
 * Phase 2 (transform): uploading a job's context file (matching
 * `CONTEXT_FILE_SUFFIX`) to the raw data bucket fires an S3 event that
 * invokes the transform dispatch Lambda directly (no queue in between,
 * unlike phase 1). It fans out to the transform worker Lambda once per
 * sibling file, merges the per-file outputs, and writes a completion
 * sentinel.
 */
export class MetadataEtlPipelineStack extends Stack {
  constructor(scope: Construct, id: string, props: MetadataEtlPipelineStackProps) {
    super(scope, id, props);

    const { config } = props;

    const rawDataBucket = new s3.Bucket(this, "RawDataBucket", {
      bucketName: `${config.rawDataBucketName}-${this.account}`,
      removalPolicy: RemovalPolicy.RETAIN,
      enforceSSL: true,
    });

    const appAsset = lambda.Code.fromAsset(path.join(__dirname, "..", "..", "..", "app"));

    // --- Phase 1: ingest --------------------------------------------------

    const ingestDlq = new sqs.Queue(this, "IngestDispatchDlq", {
      queueName: "metadata-etl-training-ingest-dispatch-dlq",
    });

    const ingestQueue = new sqs.Queue(this, "IngestDispatchQueue", {
      queueName: "metadata-etl-training-ingest-dispatch",
      visibilityTimeout: Duration.minutes(5),
      deadLetterQueue: { queue: ingestDlq, maxReceiveCount: 3 },
    });

    const ingestCallbackTopic = new sns.Topic(this, "IngestCallbackTopic", {
      topicName: "metadata-etl-training-ingest-callback",
    });

    const ingestWorkerFn = new lambda.Function(this, "IngestWorkerFunction", {
      functionName: "metadata-etl-training-ingest-worker",
      runtime: lambda.Runtime.PYTHON_3_11,
      code: appAsset,
      handler: "handlers.ingest_worker_handler.lambda_handler",
      timeout: Duration.minutes(10),
      memorySize: 512,
    });

    const ingestDispatchFn = new lambda.Function(this, "IngestDispatchFunction", {
      functionName: "metadata-etl-training-ingest-dispatch",
      runtime: lambda.Runtime.PYTHON_3_11,
      code: appAsset,
      handler: "handlers.ingest_dispatch_handler.lambda_handler",
      timeout: Duration.minutes(5),
      memorySize: 512,
      environment: {
        INGEST_WORKER_FUNCTION: ingestWorkerFn.functionName,
        STATUS_TOPIC_ARN: ingestCallbackTopic.topicArn,
      },
    });
    ingestDispatchFn.addEventSource(new events.SqsEventSource(ingestQueue, { batchSize: 1 }));
    ingestWorkerFn.grantInvoke(ingestDispatchFn);
    ingestCallbackTopic.grantPublish(ingestDispatchFn);
    rawDataBucket.grantReadWrite(ingestDispatchFn);
    rawDataBucket.grantReadWrite(ingestWorkerFn);

    // Step Functions state machine: send one SQS message per ingest job and
    // wait for the pipeline to call back via ingestCallbackTopic before
    // resuming. This is the `waitForTaskToken` pattern: the task token is
    // embedded in the SQS message payload, and the pipeline echoes it back
    // in its SNS callback publish (see app/callback.py).
    const dispatchIngestJob = new tasks.SqsSendMessage(this, "DispatchIngestJob", {
      queue: ingestQueue,
      messageBody: sfn.TaskInput.fromObject({
        token: sfn.JsonPath.taskToken,
        "input.$": "$",
      }),
      integrationPattern: sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
      taskTimeout: sfn.Timeout.duration(Duration.minutes(30)),
    });

    const ingestStateMachine = new sfn.StateMachine(this, "IngestStateMachine", {
      stateMachineName: "metadata-etl-training-ingest",
      definitionBody: sfn.DefinitionBody.fromChainable(dispatchIngestJob),
      timeout: Duration.hours(1),
    });

    // --- Phase 2: transform ------------------------------------------------

    const transformWorkerFn = new lambda.Function(this, "TransformWorkerFunction", {
      functionName: "metadata-etl-training-transform-worker",
      runtime: lambda.Runtime.PYTHON_3_11,
      code: appAsset,
      handler: "handlers.transform_worker_handler.lambda_handler",
      timeout: Duration.minutes(5),
      memorySize: 512,
    });

    const transformDispatchFn = new lambda.Function(this, "TransformDispatchFunction", {
      functionName: "metadata-etl-training-transform-dispatch",
      runtime: lambda.Runtime.PYTHON_3_11,
      code: appAsset,
      handler: "handlers.transform_dispatch_handler.lambda_handler",
      timeout: Duration.minutes(10),
      memorySize: 512,
      environment: {
        TRANSFORM_WORKER_FUNCTION: transformWorkerFn.functionName,
      },
    });
    transformWorkerFn.grantInvoke(transformDispatchFn);
    rawDataBucket.grantReadWrite(transformDispatchFn);
    rawDataBucket.grantReadWrite(transformWorkerFn);

    rawDataBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(transformDispatchFn),
      { suffix: CONTEXT_FILE_SUFFIX }
    );

    // The state machine isn't wired to CDK constructs beyond this point (the
    // callback happens out-of-band via the task token embedded in the SQS
    // message), but referencing it here keeps CDK from treating it as dead
    // code and documents the relationship for readers.
    void ingestStateMachine;
  }
}
