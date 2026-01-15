import pytest
import statistics
import numpy as np  # DODANE dla percentile
from datetime import date
from unittest.mock import MagicMock, patch
from pydantic import ValidationError
from typing import List, Dict
from models import (
    CESObservation, MacroModule, ConsumptionModule,
    LaborModule, HousingWealthModule, CreditDebtModule,
    DemographicsModule, EmploymentStatus
)
from engine import (
    AnalysisStrategy,
    GenericMeanStrategy, WeightedMeanStrategy, PercentileStrategy,
    DescriptiveStatsStrategy, AnalyticsEngine
)
from gateway import CESDataGateway

@pytest.fixture
def mock_observations() -> List[CESObservation]:
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


def test_observation_date_validation_too_early():
    """Date before 2000 should raise ValidationError."""
    with pytest.raises(ValidationError) as exc_info:
        CESObservation(
            id=1, observation_date=date(1999, 12, 31), survey_weight=1.0,
            macro=MacroModule(),
            labor=LaborModule(), consumption=ConsumptionModule(),
            housing=HousingWealthModule(), credit=CreditDebtModule(),
            demographics=DemographicsModule()
        )
    assert "observation_date year must be between 2000 and 2030" in str(exc_info.value)


def test_observation_date_validation_too_late():
    """Date after 2030 should raise ValidationError."""
    with pytest.raises(ValidationError) as exc_info:
        CESObservation(
            id=1, observation_date=date(2031, 1, 1), survey_weight=1.0,
            macro=MacroModule(),
            labor=LaborModule(), consumption=ConsumptionModule(),
            housing=HousingWealthModule(), credit=CreditDebtModule(),
            demographics=DemographicsModule()
        )
    assert "observation_date year must be between 2000 and 2030" in str(exc_info.value)


def test_observation_date_validation_valid():
    """Valid dates (2015) should pass without error."""
    obs = CESObservation(
        id=1, observation_date=date(2015, 6, 15), survey_weight=1.0,
        macro=MacroModule(),
        labor=LaborModule(), consumption=ConsumptionModule(),
        housing=HousingWealthModule(), credit=CreditDebtModule(),
        demographics=DemographicsModule()
    )
    assert obs.observation_date.year == 2015
    assert obs.observation_date.month == 6


def test_weighted_mean_strategy(mock_observations):
    """
    WeightedMeanStrategy should account for survey_weight.
    obs1: inflation=2.0, weight=1.5 → weighted=3.0
    obs2: inflation=4.0, weight=1.0 → weighted=4.0
    Result: (3.0 + 4.0) / (1.5 + 1.0) = 7.0 / 2.5 = 2.8
    """
    strategy = WeightedMeanStrategy("macro.inflation_1y")
    result = strategy.compute(mock_observations)
    assert result == pytest.approx(2.8, abs=0.01)


def test_percentile_strategy_median(mock_observations):
    """PercentileStrategy with p=0.5 should return median."""
    strategy = PercentileStrategy("macro.inflation_1y", percentile=0.5)
    result = strategy.compute(mock_observations)
    #Values: [2.0, 4.0], median = 3.0
    assert result == pytest.approx(3.0, abs=0.1)


def test_percentile_strategy_q1(mock_observations):
    """PercentileStrategy with p=0.25 should return first quartile."""
    strategy = PercentileStrategy("macro.inflation_1y", percentile=0.25)
    result = strategy.compute(mock_observations)
    #Values: [2.0, 4.0], Q1 should be closer to 2.0
    assert result >= 2.0 and result <= 3.0


def test_percentile_strategy_q3(mock_observations):
    """PercentileStrategy with p=0.75 should return third quartile."""
    strategy = PercentileStrategy("macro.inflation_1y", percentile=0.75)
    result = strategy.compute(mock_observations)
    #Values: [2.0, 4.0], Q3 should be closer to 4.0
    assert result >= 3.0 and result <= 4.0


def test_descriptive_stats_strategy(mock_observations):
    """DescriptiveStatsStrategy should return full statistics dict."""
    strategy = DescriptiveStatsStrategy("macro.inflation_1y")
    result = strategy.compute(mock_observations)
    
    assert isinstance(result, dict)
    assert "min" in result and "max" in result
    assert "mean" in result and "median" in result
    assert "stdev" in result and "count" in result
    
    #Verify values: [2.0, 4.0]
    assert result["min"] == 2.0
    assert result["max"] == 4.0
    assert result["mean"] == pytest.approx(3.0, abs=0.01)
    assert result["median"] == pytest.approx(3.0, abs=0.1)
    assert result["count"] == 2
    assert result["stdev"] == pytest.approx(1.414, abs=0.01)

def test_empty_data_list():
    """All strategies should handle empty data gracefully."""
    strategy = GenericMeanStrategy("macro.inflation_1y")
    assert strategy.compute([]) == 0.0
    
    weighted_strategy = WeightedMeanStrategy("macro.inflation_1y")
    assert weighted_strategy.compute([]) == 0.0
    
    desc_strategy = DescriptiveStatsStrategy("macro.inflation_1y")
    result = desc_strategy.compute([])
    assert result["count"] == 0
    assert result["stdev"] is None
    assert "min" in result 

def test_all_none_values():
    """Strategies should return 0.0 when all values are None."""
    obs = CESObservation(
        id=1, observation_date=date(2024, 1, 1), survey_weight=1.0,
        macro=MacroModule(inflation_1y=0.0),
        labor=LaborModule(), consumption=ConsumptionModule(),
        housing=HousingWealthModule(), credit=CreditDebtModule(),
        demographics=DemographicsModule()
    )
    
    strategy = GenericMeanStrategy("macro.inflation_1y")
    assert strategy.compute([obs]) == 0.0
    
    weighted_strategy = WeightedMeanStrategy("macro.inflation_1y")
    assert weighted_strategy.compute([obs]) == 0.0


def test_mixed_none_and_values():
    """Strategies should skip None values and compute on valid ones."""
    obs1 = CESObservation(
        id=1, observation_date=date(2024, 1, 1), survey_weight=1.0,
        macro=MacroModule(inflation_1y=0.0),
        labor=LaborModule(), consumption=ConsumptionModule(),
        housing=HousingWealthModule(), credit=CreditDebtModule(),
        demographics=DemographicsModule()
    )
    obs2 = CESObservation(
        id=2, observation_date=date(2024, 1, 1), survey_weight=1.0,
        macro=MacroModule(inflation_1y=5.0),
        labor=LaborModule(), consumption=ConsumptionModule(),
        housing=HousingWealthModule(), credit=CreditDebtModule(),
        demographics=DemographicsModule()
    )
    
    strategy = GenericMeanStrategy("macro.inflation_1y")
    assert strategy.compute([obs1, obs2]) == 2.5


def test_filter_by_date_range():
    """Test date range filtering in AnalyticsEngine."""
    obs1 = CESObservation(
        id=1, observation_date=date(2020, 1, 1), survey_weight=1.0,
        macro=MacroModule(),
        labor=LaborModule(), consumption=ConsumptionModule(),
        housing=HousingWealthModule(), credit=CreditDebtModule(),
        demographics=DemographicsModule()
    )
    obs2 = CESObservation(
        id=2, observation_date=date(2022, 6, 15), survey_weight=1.0,
        macro=MacroModule(),
        labor=LaborModule(), consumption=ConsumptionModule(),
        housing=HousingWealthModule(), credit=CreditDebtModule(),
        demographics=DemographicsModule()
    )
    obs3 = CESObservation(
        id=3, observation_date=date(2025, 12, 31), survey_weight=1.0,
        macro=MacroModule(),
        labor=LaborModule(), consumption=ConsumptionModule(),
        housing=HousingWealthModule(), credit=CreditDebtModule(),
        demographics=DemographicsModule()
    )
    
    engine = AnalyticsEngine()
    
    filtered = engine.filter_by_date([obs1, obs2, obs3], 2021, 2024)
    assert len(filtered) == 1
    assert filtered[0].id == 2
    
    filtered = engine.filter_by_date([obs1, obs2, obs3], 2020, 2020)
    assert len(filtered) == 1
    assert filtered[0].id == 1

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
