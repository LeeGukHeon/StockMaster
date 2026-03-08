from app.intraday.meta_common import ENTER_PANEL, WAIT_PANEL
from app.intraday.meta_inference import _action_from_prediction


def test_action_from_prediction_for_enter_panel():
    assert _action_from_prediction(ENTER_PANEL, "KEEP_ENTER", "ENTER_NOW") == "ENTER_NOW"
    assert _action_from_prediction(ENTER_PANEL, "DOWNGRADE_WAIT", "ENTER_NOW") == "WAIT_RECHECK"
    assert _action_from_prediction(ENTER_PANEL, "DOWNGRADE_AVOID", "ENTER_NOW") == "AVOID_TODAY"


def test_action_from_prediction_for_wait_panel():
    assert _action_from_prediction(WAIT_PANEL, "KEEP_WAIT", "WAIT_RECHECK") == "WAIT_RECHECK"
    assert _action_from_prediction(WAIT_PANEL, "UPGRADE_ENTER", "WAIT_RECHECK") == "ENTER_NOW"
    assert _action_from_prediction(WAIT_PANEL, "DOWNGRADE_AVOID", "WAIT_RECHECK") == "AVOID_TODAY"
