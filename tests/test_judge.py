import asyncio

from judge import MockJudge


def test_mock_judge_routes_urgent_locked_account_to_security() -> None:
    result = asyncio.run(MockJudge().evaluate("URGENT: my account is locked"))

    assert result.urgency == 0.92
    assert result.department == "security"


def test_mock_judge_defaults_to_nonurgent_technical() -> None:
    result = asyncio.run(MockJudge().evaluate("How do I export my data?"))

    assert result.urgency == 0.42
    assert result.department == "technical"


def test_mock_judge_routes_billing_terms() -> None:
    result = asyncio.run(MockJudge().evaluate("I need a refund for this invoice"))

    assert result.department == "billing"