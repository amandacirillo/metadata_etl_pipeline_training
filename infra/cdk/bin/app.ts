#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { MetadataEtlPipelineStack } from "../lib/stack";
import { ENV_CONFIGS } from "../lib/constants";

const app = new cdk.App();

const envName = app.node.tryGetContext("envName") ?? "dev";
const config = ENV_CONFIGS[envName];
if (!config) {
  throw new Error(`Unknown envName "${envName}". Expected one of: ${Object.keys(ENV_CONFIGS).join(", ")}`);
}

new MetadataEtlPipelineStack(app, `MetadataEtlPipelineTrainingStack-${config.envName}`, {
  config,
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION ?? "us-east-1",
  },
});
