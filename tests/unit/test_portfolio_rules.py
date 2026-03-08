from app.portfolio.allocation import _action_from_delta


def test_action_from_delta_mapping():
    assert _action_from_delta(0, 10) == "BUY_NEW"
    assert _action_from_delta(10, 15) == "ADD"
    assert _action_from_delta(10, 10) == "HOLD"
    assert _action_from_delta(10, 5) == "TRIM"
    assert _action_from_delta(10, 0) == "EXIT"
    assert _action_from_delta(0, 0) == "NO_ACTION"
