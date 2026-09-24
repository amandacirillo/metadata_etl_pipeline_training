"""Build and publish the message that resumes a waiting Step Functions
execution (the ``waitForTaskToken`` service-integration pattern).

A Step Functions state machine can dispatch work asynchronously (here: send
an SQS message) and then pause, holding a ``taskToken``, until something
calls ``SendTaskSuccess``/``SendTaskFailure`` with that same token. This
pipeline's dispatcher does the equivalent by publishing an SNS message
(a subscription on the topic forwards it into the appropriate Step
Functions callback) once all of its fanned-out worker invocations finish.
"""
from typing import Dict, List

from app.aws_clients import SnsPublisher

STATUS_COMPLETE = "COMPLETE"
STATUS_ERROR = "ERROR"


def build_callback_message(job_id: str, task_token: str, results: Dict[int, dict]) -> dict:
    """Summarize a `fan_out` results dict into a Step Functions callback payload.

    Args:
        job_id: The ID of the overall job being processed.
        task_token: The Step Functions task token to resume.
        results: The dict returned by :func:`app.fanout.fan_out`.

    Returns:
        ``{"status": "COMPLETE"|"ERROR", "jobId", "taskToken", "errorDetails": [...]}``.
        ``status`` is ``"ERROR"`` if any individual result failed; every
        failure is recorded in ``errorDetails`` even when others succeeded.
    """
    error_details: List[dict] = []
    for result in results.values():
        if "errorMessage" in result:
            error_details.append({"type": result.get("errorType", "ERROR"), "message": result["errorMessage"]})
        elif result.get("status") == "ERROR":
            error_details.append({"type": "ERROR", "message": result.get("message", "unknown error")})

    return {
        "status": STATUS_ERROR if error_details else STATUS_COMPLETE,
        "jobId": job_id,
        "taskToken": task_token,
        "errorDetails": error_details,
    }


def publish_callback(sns_client: SnsPublisher, topic_arn: str, message: dict) -> None:
    sns_client.publish(topic_arn, message)
