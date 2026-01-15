import csv
import aiofiles
from datetime import datetime
from typing import List, Optional
from models import *
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class CESDataGateway:
    def __init__(self, file_path: str):
        self.file_path = file_path
        logger.info(f"CESDataGateway initialized with file: {file_path}")

    def _f(self, row, key) -> Optional[float]:
        val = row.get(key, "").strip()
        if not val or val.lower() == "none":
            return None
        try:
            return float(val)
        except ValueError:
            return None

    def _i(self, row, key) -> Optional[int]:
        res = self._f(row, key)
        return int(round(res)) if res is not None else None

    VARIABLE_MAP = {
    "inflation": "macro.inflation_1y",
    "c4030": "macro.inflation_1y",
    "inflation_3y": "macro.inflation_3y",
    "c4031": "macro.unemployment_percept",
    "unemployment": "macro.unemployment_percept",
    "inflation_perception": "macro.inflation_perception_12m",
    "c4010": "macro.inflation_perception_12m",
    "inflation_uncertainty": "macro.inflation_uncertainty",
    "c4020": "macro.inflation_uncertainty",
    "gdp_growth": "macro.econ_growth_12m",
    "e2010": "macro.econ_growth_12m",
    "interest_rate": "macro.interest_rate_exp",
    "e2020": "macro.interest_rate_exp",
    "income": "consumption.income_growth",
    "c1150_1": "consumption.income_growth",
    "spending": "consumption.spending_growth",
    "c1150_2": "consumption.spending_growth",    
    "major_purchases": "consumption.major_purchases_intent",
    "c1150_3": "consumption.major_purchases_intent",
    "savings": "consumption.savings_growth",
    "c1220": "consumption.savings_growth",
    "tax_exp": "consumption.tax_exp",
    "c1150_4": "consumption.tax_exp",
    "public_services": "consumption.public_svc_exp",
    "c1150_5": "consumption.public_svc_exp",
    "housing_costs": "consumption.housing_costs_exp",
    "c1150_6": "consumption.housing_costs_exp",
    "energy": "consumption.energy_exp",
    "c1150_7": "consumption.energy_exp",
    "food": "consumption.food_exp",
    "c1150_8": "consumption.food_exp",
    "job_loss": "labor.job_loss_prob",
    "p1410_1": "labor.job_loss_prob",
    "job_find": "labor.job_find_prob",
    "p1410_2": "labor.job_find_prob",
    "work_after_70": "labor.prob_working_after_70",
    "p1410_3": "labor.prob_working_after_70",
    "income_increase": "labor.prob_inc_increase",
    "p1410_4": "labor.prob_inc_increase",
    "leave_labor": "labor.prob_leaving_labor_market",
    "p1410_5": "labor.prob_leaving_labor_market",
    "house_prices": "housing.house_price_exp",
    "c3220": "housing.house_price_exp",
    "buy_home": "housing.buy_home_conditions",
    "c3210": "housing.buy_home_conditions",
    "cash": "housing.cash_savings",
    "c3250_1": "housing.cash_savings",
    "life_insurance": "housing.life_insurance",
    "c3250_2": "housing.life_insurance",
    "pensions": "housing.pension_funds",
    "c3250_3": "housing.pension_funds",
    "mutual_funds": "housing.mutual_funds",
    "c3250_4": "housing.mutual_funds",
    "gov_bonds": "housing.public_bonds",
    "c3250_5": "housing.public_bonds",
    "corp_bonds": "housing.corp_bonds",
    "c3250_6": "housing.corp_bonds",
    "stocks": "housing.stocks",
    "c3250_7": "housing.stocks",
    "business": "housing.business_equity",
    "c3250_8": "housing.business_equity",
    "crypto": "housing.crypto_assets",
    "c3251_9": "housing.crypto_assets",
    "gold": "housing.precious_metals",
    "c3251_10": "housing.precious_metals",
    "mortgage": "credit.mortgage_access",
    "c8010_1": "credit.mortgage_access",
    "consumer_credit": "credit.consumer_credit_access",
    "c8010_2": "credit.consumer_credit_access",
    "bank_loan": "credit.bank_loan_apply",
    "c8010_3": "credit.bank_loan_apply",
    "credit_constraint": "credit.credit_constraint_flag",
    "c8011_1": "credit.credit_constraint_flag",
    "gender": "demographics.gender",
    "c1010": "demographics.gender",
    "age_group": "demographics.age_group",
    "c1020": "demographics.age_group",
    "household_size": "demographics.household_size",
    "c2110": "demographics.household_size",
    "children": "demographics.num_children",
    "c2120": "demographics.num_children",
    "age": "demographics.household_members[0].age",
    "c2151_1": "demographics.household_members[0].age",
    "education": "demographics.occupation.edu_level",
    "x1040_1": "demographics.occupation.edu_level",
    "sector": "demographics.occupation.employment_sector",
    "x1040_2": "demographics.occupation.employment_sector",
}


    async def load_all_data(self) -> List[CESObservation]:
        logger.info(f"Loading data from {self.file_path}")
        observations: List[CESObservation] = []

        try:
            async with aiofiles.open(self.file_path, mode="r", encoding="utf-8") as f:
                content = await f.read()
            logger.debug(f"File read successfully, size: {len(content)} bytes")

            reader = csv.DictReader(content.splitlines())
            row_count = 0

            for i, row in enumerate(reader):
                row_count += 1

                #Macro
                macro = MacroModule(
                    inflation_1y=self._f(row, "c4030") or 0.0,
                    inflation_3y=self._f(row, "c4031"),
                    inflation_perception_12m=self._f(row, "c4010"),
                    inflation_uncertainty=self._f(row, "c4020"),
                    econ_growth_12m=self._f(row, "e2010"),
                    interest_rate_exp=self._f(row, "e2020"),
                    unemployment_percept=self._f(row, "c4032"),
                )

                #Consumption
                cons_raw = {
                    f"comp_{j}": self._f(row, f"c1150_{j}") for j in range(4, 9)
                }
                cons = ConsumptionModule(
                    income_growth=self._f(row, "c1150_1"),
                    spending_growth=self._f(row, "c1150_2"),
                    major_purchases_intent=self._f(row, "c1150_3"),
                    savings_growth=self._f(row, "c1220"),
                    tax_exp=self._f(row, "c1150_4"),
                    public_svc_exp=self._f(row, "c1150_5"),
                    housing_costs_exp=self._f(row, "c1150_6"),
                    energy_exp=self._f(row, "c1150_7"),
                    food_exp=self._f(row, "c1150_8"),
                )

                #Labor
                raw_emp = row.get("emp_status", "").strip()
                try:
                    emp_val = str(round(float(raw_emp))) if raw_emp else "unknown"
                    emp_status_enum = EmploymentStatus(emp_val)
                except (ValueError, TypeError):
                    emp_status_enum = EmploymentStatus.UNKNOWN

                labor = LaborModule(
                    job_loss_prob=self._f(row, "p1410_1"),
                    job_find_prob=self._f(row, "p1410_2"),
                    prob_working_after_70=self._f(row, "p1410_3"),
                    prob_inc_increase=self._f(row, "p1410_4"),
                    prob_leaving_labor_market=self._f(row, "p1410_5"),
                    employment_status=emp_status_enum,
                )

                #Housing & Wealth
                assets_raw = {
                    k: self._f(row, k)
                    for k in row
                    if "c3250" in k or "c3251" in k
                }
                housing = HousingWealthModule(
                    house_price_exp=self._f(row, "c3220"),
                    buy_home_conditions=self._f(row, "c3210"),
                    cash_savings=self._f(row, "c3250_1"),
                    life_insurance=self._f(row, "c3250_2"),
                    pension_funds=self._f(row, "c3250_3"),
                    mutual_funds=self._f(row, "c3250_4"),
                    public_bonds=self._f(row, "c3250_5"),
                    corp_bonds=self._f(row, "c3250_6"),
                    stocks=self._f(row, "c3250_7"),
                    business_equity=self._f(row, "c3250_8"),
                    crypto_assets=self._f(row, "c3251_9"),
                    precious_metals=self._f(row, "c3251_10"),
                    other_financial_details={
                        f"c3251_{j}": self._f(row, f"c3251_{j}")
                        for j in range(1, 9)
                        if self._f(row, f"c3251_{j}") is not None
                    },
                )

                # Demographics
                occ = OccupationalProfile(
                    edu_level=row.get("x1040_1"),
                    employment_sector=row.get("x1040_2"),
                    occupation_type=row.get("x1040_3"),
                    industry=row.get("x1040_4"),
                    hours_worked=row.get("x1040_5"),
                    firm_size=row.get("x1040_6"),
                    contract_type=row.get("x1040_7"),
                    tenure=row.get("x1040_8"),
                    public_private=row.get("x1040_9"),
                )

                members: List[HouseholdMember] = []
                for m_idx in range(1, 11):
                    age_val = self._f(row, f"c2151_{m_idx}")
                    rel_val = self._f(row, f"c2150_{m_idx}")
                    if age_val is not None or rel_val is not None:
                        members.append(
                            HouseholdMember(
                                member_id=m_idx,
                                age=age_val,
                                relation_status=rel_val,
                            )
                        )

                demo = DemographicsModule(
                    gender=self._i(row, "c1010"),
                    age_group=self._i(row, "c1020"),
                    household_size=self._i(row, "c2110"),
                    num_children=self._i(row, "c2120"),
                    occupation=occ,
                    household_members=members,
                )

                credit_raw = {
                    f"debt_{j}": self._f(row, f"c8011_{j}") for j in [2, 4, 5]
                }

                obs = CESObservation(
                    id=i,
                    observation_date=datetime.strptime(
                        row["Date"], "%Y-%m-%d"
                    ).date(),
                    macro=macro,
                    consumption=cons,
                    labor=labor,
                    housing=housing,
                    demographics=demo,
                    credit=CreditDebtModule(
                        mortgage_access=self._f(row, "c8010_1"),
                        consumer_credit_access=self._f(row, "c8010_2"),
                        bank_loan_apply=self._f(row, "c8010_3"),
                        credit_constraint_flag=self._f(row, "c8011_1"),
                        loan_repayment_struggle=self._f(row, "c8011_3"),
                        debt_details={
                            k: v for k, v in credit_raw.items() if v is not None
                        },
                    ),
                    survey_weight=self._f(row, "wgt") or 1.0,
                    sentiment_flags={
                        k: row[k]
                        for k in row
                        if any(x in k for x in ["x6020", "h2020", "c60", "x8020", "x8110"])
                    },
                    quarterly_data={k: row[k] for k in row if k.endswith("_q")},
                    quality_audit={k: row[k] for k in row if k.endswith("_nr")},
                )

                observations.append(obs)

            logger.info(f"Successfully loaded {row_count} observations")
            return observations

        except FileNotFoundError:
            logger.error(f"File not found: {self.file_path}")
            raise
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}", exc_info=True)
            raise
