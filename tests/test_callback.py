from app.callback import build_callback_message, publish_callback
from tests.fakes import FakeSnsPublisher


def test_build_callback_message_complete_when_no_errors():
    results = {0: {"status": "OK"}, 1: {"status": "OK"}}
    message = build_callback_message("JOB1", "token-abc", results)

    assert message == {"status": "COMPLETE", "jobId": "JOB1", "taskToken": "token-abc", "errorDetails": []}


def test_build_callback_message_error_when_any_result_failed():
    results = {0: {"status": "OK"}, 1: {"errorType": "RuntimeError", "errorMessage": "boom"}}
    message = build_callback_message("JOB1", "token-abc", results)

    assert message["status"] == "ERROR"
    assert message["errorDetails"] == [{"type": "RuntimeError", "message": "boom"}]


def test_build_callback_message_collects_every_error():
    results = {
        0: {"errorType": "A", "errorMessage": "first"},
        1: {"status": "OK"},
        2: {"errorType": "B", "errorMessage": "second"},
    }
    message = build_callback_message("JOB1", "token-abc", results)

    assert message["status"] == "ERROR"
    assert len(message["errorDetails"]) == 2


def test_publish_callback_sends_message_to_topic():
    sns = FakeSnsPublisher()
    publish_callback(sns, "arn:aws:sns:us-east-1:123:topic", {"status": "COMPLETE"})

    assert sns.published == [("arn:aws:sns:us-east-1:123:topic", {"status": "COMPLETE"})]
