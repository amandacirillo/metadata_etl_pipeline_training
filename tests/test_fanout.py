from app.fanout import fan_out, has_errors
from tests.fakes import FakeLambdaInvoker


def test_fan_out_invokes_once_per_item_and_collects_results():
    invoker = FakeLambdaInvoker({"worker-fn": lambda payload: {"status": "OK", "echoed": payload["n"]}})
    items = [{"n": 1}, {"n": 2}, {"n": 3}]

    results = fan_out("worker-fn", items, invoker)

    assert len(results) == 3
    assert {r["echoed"] for r in results.values()} == {1, 2, 3}
    assert len(invoker.calls) == 3


def test_fan_out_isolates_one_item_failure():
    def flaky(payload):
        if payload["n"] == 2:
            raise RuntimeError("boom")
        return {"status": "OK"}

    invoker = FakeLambdaInvoker({"worker-fn": flaky})
    results = fan_out("worker-fn", [{"n": 1}, {"n": 2}, {"n": 3}], invoker)

    assert len(results) == 3
    assert has_errors(results) is True
    failing = [r for r in results.values() if "errorMessage" in r]
    assert len(failing) == 1
    assert "boom" in failing[0]["errorMessage"]


def test_fan_out_with_no_items_returns_empty_dict():
    invoker = FakeLambdaInvoker({})
    assert fan_out("worker-fn", [], invoker) == {}


def test_has_errors_false_when_all_succeed():
    invoker = FakeLambdaInvoker({"worker-fn": lambda payload: {"status": "OK"}})
    results = fan_out("worker-fn", [{"n": 1}], invoker)
    assert has_errors(results) is False
