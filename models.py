from enum import StrEnum
from datetime import date
from typing import Optional, Dict, List, Any
from pydantic.dataclasses import dataclass
from pydantic import Field, ConfigDict
from pydantic import field_validator
from pydantic import Field, ConfigDict, field_validator

class EmploymentStatus(StrEnum):
    EMPLOYED = "1"
    UNEMPLOYED = "2"
    INACTIVE = "3"
    OTHER = "4"
    UNKNOWN = "unknown"
@dataclass
class HouseholdMember:
    """Represents a member of the respondent's household (c2150/c2151 series)."""
    member_id: int
    age: Optional[float] = None #c2150_x
    relation_status: Optional[float] = None #c2151_x

@dataclass
class OccupationalProfile:
    """Detailed job and education data (x1040 series)."""
    edu_level: Optional[str] = None #x1040_1
    employment_sector: Optional[str] = None #x1040_2
    occupation_type: Optional[str] = None #x1040_3
    industry: Optional[str] = None #x1040_4
    hours_worked: Optional[str] = None #x1040_5
    firm_size: Optional[str] = None #x1040_6
    contract_type: Optional[str] = None #x1040_7
    tenure: Optional[str] = None #x1040_8
    public_private: Optional[str] = None #x1040_9

@dataclass
class DemographicsModule:
    gender: Optional[int] = None #c1010
    age_group: Optional[int] = None #c1020
    household_size: Optional[int] = None #c2110
    num_children: Optional[int] = None #c2120
    occupation: OccupationalProfile = Field(default_factory=OccupationalProfile)
    household_members: List[HouseholdMember] = Field(default_factory=list)

@dataclass
class MacroModule:
    inflation_1y: float = Field(default=0.0, ge=-100, le=100) #c4030
    inflation_3y: Optional[float] = None #c4031
    inflation_perception_12m: Optional[float] = None #c4010
    inflation_uncertainty: Optional[float] = None #c4020
    econ_growth_12m: Optional[float] = None #e2010
    interest_rate_exp: Optional[float] = None  #e2020
    unemployment_percept: Optional[float] = None #c4032

@dataclass
class ConsumptionModule:
    income_growth: Optional[float] = None #c1150_1
    spending_growth: Optional[float] = None #c1150_2
    major_purchases_intent: Optional[float] = None #c1150_3
    savings_growth: Optional[float] = None #c1220
    tax_exp: Optional[float] = None #c1150_4
    public_svc_exp: Optional[float] = None #c1150_5
    housing_costs_exp: Optional[float] = None #c1150_6
    energy_exp: Optional[float] = None #c1150_7
    food_exp: Optional[float] = None #c1150_8

@dataclass
class LaborModule:
    job_loss_prob: Optional[float] = None #p1410_1
    job_find_prob: Optional[float] = None #p1410_2
    prob_working_after_70: Optional[float] = None #p1410_3
    prob_inc_increase: Optional[float] = None #p1410_4
    prob_leaving_labor_market: Optional[float] = None #p1410_5
    employment_status: EmploymentStatus = EmploymentStatus.UNKNOWN

@dataclass
class HousingWealthModule:
    house_price_exp: Optional[float] = None #c3220
    buy_home_conditions: Optional[float] = None #c3210
    cash_savings: Optional[float] = None #c3250_1
    life_insurance: Optional[float] = None #c3250_2
    pension_funds: Optional[float] = None #c3250_3
    mutual_funds: Optional[float] = None #c3250_4
    public_bonds: Optional[float] = None #c3250_5
    corp_bonds: Optional[float] = None #c3250_6
    stocks: Optional[float] = None #c3250_7
    business_equity: Optional[float] = None #c3250_8
    crypto_assets: Optional[float] = None #c3251_9
    precious_metals: Optional[float] = None #c3251_10
    other_financial_details: Dict[str, float] = Field(default_factory=dict)

@dataclass
class CreditDebtModule:
    mortgage_access: Optional[float] = None #c8010_1
    consumer_credit_access: Optional[float] = None #c8010_2
    bank_loan_apply: Optional[float] = None #c8010_3
    credit_constraint_flag: Optional[float] = None #c8011_1
    repayment_increase_prob: Optional[float] = None #c8011_2
    loan_repayment_struggle: Optional[float] = None #c8011_3
    debt_level_expected: Optional[float] = None #c8011_4
    repayment_capacity: Optional[float] = None #c8011_5

@dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class CESObservation:
    id: int
    observation_date: date
    macro: MacroModule
    consumption: ConsumptionModule
    labor: LaborModule
    housing: HousingWealthModule
    credit: CreditDebtModule
    demographics: DemographicsModule
    survey_weight: float = Field(default=1.0, gt=0)
    sentiment_flags: Dict[str, Any] = Field(default_factory=dict)
    quarterly_data: Dict[str, Any] = Field(default_factory=dict)
    quality_audit: Dict[str, Any] = Field(default_factory=dict)
    
    @field_validator('observation_date')
    @classmethod
    def validate_date_range(cls, v):
        """Ensure observation_date is within reasonable range (2000-2030)."""
        if v.year < 2000 or v.year > 2030:
            raise ValueError(
                f"observation_date year must be between 2000 and 2030, got {v.year}"
            )
        return v
    
    @field_validator('survey_weight')
    @classmethod
    def validate_weight_positive(cls, v):
        """Ensure survey_weight is strictly positive."""
        if v <= 0:
            raise ValueError(f"survey_weight must be > 0, got {v}")
        return v


