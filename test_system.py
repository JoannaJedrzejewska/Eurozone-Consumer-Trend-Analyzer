import pytest
from datetime import date
from unittest.mock import MagicMock, patch
from pydantic import ValidationError

from models import (
    CESObservation, MacroModule, ConsumptionModule, 
    LaborModule, HousingWealthModule, CreditDebtModule,
    DemographicsModule, EmploymentStatus
)
from engine import GenericMeanStrategy, AnalyticsEngine
from gateway import CESDataGateway


@pytest.fixture
def mock_observations():
    """Creates a small collection of valid objects for testing math strategies."""
    obs1 = CESObservation(
        id=1, observation_date=date(2024, 1, 1), survey_weight=1.5,
        macro=MacroModule(inflation_1y=2.0, unemployment_percept=5.0),
        labor=LaborModule(job_loss_prob=10.0),
        consumption=ConsumptionModule(income_growth=2.0), 
        housing=HousingWealthModule(),
        credit=CreditDebtModule(), 
        demographics=DemographicsModule()
    )
    obs2 = CESObservation(
        id=2, observation_date=date(2024, 1, 1), survey_weight=1.0,
        macro=MacroModule(inflation_1y=4.0, unemployment_percept=10.0),
        labor=LaborModule(job_loss_prob=20.0),
        consumption=ConsumptionModule(income_growth=4.0), 
        housing=HousingWealthModule(),
        credit=CreditDebtModule(), 
        demographics=DemographicsModule()
    )
    return [obs1, obs2]


def test_weight_must_be_positive():
    """Verify that Pydantic catches invalid survey weights (gt=0)."""
    with pytest.raises(ValidationError):
        #This should fail because weight is -5.0
        CESObservation(
            id=1, observation_date=date(2024, 1, 1), survey_weight=-5.0,
            macro=MacroModule(inflation_1y=2.0),
            labor=LaborModule(), consumption=ConsumptionModule(),
            housing=HousingWealthModule(), credit=CreditDebtModule(),
            demographics=DemographicsModule()
        )


def test_inflation_strategy_calculation(mock_observations):
    """Checks if the GenericMeanStrategy computes the correct mean for inflation."""
    strategy = GenericMeanStrategy("macro.inflation_1y")
    result = strategy.compute(mock_observations)
    #(2.0 + 4.0) / 2 = 3.0
    assert result == 3.0

def test_income_growth_strategy_calculation(mock_observations):
    """Checks mean for income growth."""
    strategy = GenericMeanStrategy("consumption.income_growth")
    result = strategy.compute(mock_observations)
    #(2.0 + 4.0) / 2 = 3.0
    assert result == 3.0


def test_gateway_safe_float():
    """Tests the internal safe float helper in the gateway."""
    gateway = CESDataGateway("ces_data.csv")
    assert gateway._f({"val": "12.5"}, "val") == 12.5
    assert gateway._f({"val": ""}, "val") is None
    assert gateway._f({"val": "not_a_number"}, "val") is None

if __name__ == "__main__":
    import pytest
    sys_exit_code = pytest.main([__file__, "-v"])