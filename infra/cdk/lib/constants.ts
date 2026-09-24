/**
 * Fictional per-environment configuration for the metadata ETL pipeline
 * training stack. No values here reflect real accounts, VPCs, or network
 * ranges -- they exist only to make the stack synthesizable and to show the
 * *shape* of environment-specific configuration you'd plumb through in a
 * real deployment.
 */
export interface EnvConfig {
  readonly envName: string;
  readonly rawDataBucketName: string;
  readonly vpcCidr: string;
}

export const ENV_CONFIGS: Record<string, EnvConfig> = {
  dev: {
    envName: "dev",
    rawDataBucketName: "metadata-etl-training-raw-dev",
    vpcCidr: "10.20.0.0/16",
  },
  prod: {
    envName: "prod",
    rawDataBucketName: "metadata-etl-training-raw-prod",
    vpcCidr: "10.21.0.0/16",
  },
};

// Suffix that marks a "batch ready" context file upload, triggering the
// transform phase. Mirrors the real pipeline's use of a distinctive file
// suffix as a poor-man's event filter on S3 notifications.
export const CONTEXT_FILE_SUFFIX = "_CONTEXT_OUTPUT.json";
