import csv
import aiofiles
from datetime import datetime
from typing import List, Optional
from models import *

class CESDataGateway:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def _f(self, row, key) -> Optional[float]:
        val = row.get(key, "").strip()
        if not val: return None
        try:
            return float(val)
        except ValueError:
            return None

    def _i(self, row, key) -> Optional[int]:
        res = self._f(row, key)
        return int(res) if res is not None else None
#to clean/append
    VARIABLE_MAP = {
        "inflation": "macro.inflation_1y",
        "income": "consumption.income_growth",
        "spending": "consumption.spending_growth",
        "food": "consumption.food_exp",
        "energy": "consumption.energy_exp",
        "housing_costs": "consumption.housing_costs_exp",
        "house_prices": "housing.house_price_exp",
        "savings": "consumption.savings_growth",
        "spending": "consumption.spending_growth",
        "c1150_6": "consumption.housing_costs_exp",
        "c1150_2": "consumption.spending_growth",
        "job_find": "labor.job_find_prob",
        "stocks": "housing.stocks",
        "mortgage": "credit.mortgage_access",
        "crypto": "housing.crypto_assets",
        "job_loss": "labor.job_loss_prob",
        "c4030": "macro.inflation_1y",
        "c1150_1": "consumption.income_growth",
        "c3220": "housing.house_price_exp",
        "c3220": "housing.house_price_exp",
        "p1410_1": "labor.job_loss_prob",
        "age": "c2151_1"
    
    }
    def _f(self, row, key) -> Optional[float]:
        val = row.get(key, "").strip()
        if not val or val.lower() == "none": return None
        try: return float(val)
        except ValueError: return None

    def _i(self, row, key) -> Optional[int]:
        res = self._f(row, key)
        return int(round(res)) if res is not None else None

    async def load_all_data(self) -> List[CESObservation]:
        observations = []
        async with aiofiles.open(self.file_path, mode='r', encoding='utf-8') as f:
            content = await f.read()
            reader = csv.DictReader(content.splitlines())
            
            for i, row in enumerate(reader):
                #Macro
                macro = MacroModule(
                    inflation_1y=self._f(row, 'c4030') or 0.0,
                    inflation_3y=self._f(row, 'c4031'),
                    inflation_perception_12m=self._f(row, 'c4010'),
                    inflation_uncertainty=self._f(row, 'c4020'),
                    econ_growth_12m=self._f(row, 'e2010'),
                    interest_rate_exp=self._f(row, 'e2020'),
                    unemployment_percept=self._f(row, 'c4030')
                )

                #Consumption
                cons_raw = {f"comp_{j}": self._f(row, f"c1150_{j}") for j in range(4, 9)}
                cons = ConsumptionModule(
                    income_growth=self._f(row, 'c1150_1'),
                    spending_growth=self._f(row, 'c1150_2'),
                    major_purchases_intent=self._f(row, 'c1150_3'),
                    savings_growth=self._f(row, 'c1220'),
                    tax_exp=self._f(row, 'c1150_4'),
                    public_svc_exp=self._f(row, 'c1150_5'),
                    housing_costs_exp=self._f(row, 'c1150_6'),
                    energy_exp=self._f(row, 'c1150_7'),
                    food_exp=self._f(row, 'c1150_8')
                )

                #Labor
                raw_emp = row.get('emp_status', '').strip()
                try:
                    emp_val = str(round(float(raw_emp))) if raw_emp else "unknown"
                    emp_status_enum = EmploymentStatus(emp_val)
                except (ValueError, TypeError):
                    emp_status_enum = EmploymentStatus.UNKNOWN

                labor = LaborModule(
                    job_loss_prob=self._f(row, 'p1410_1'),
                    job_find_prob=self._f(row, 'p1410_2'),
                    prob_working_after_70=self._f(row, 'p1410_3'),
                    prob_inc_increase=self._f(row, 'p1410_4'),
                    prob_leaving_labor_market=self._f(row, 'p1410_5'),
                    employment_status=emp_status_enum
                )

                #Housing & Wealth
                assets_raw = {k: self._f(row, k) for k in row if 'c3250' in k or 'c3251' in k}
                housing = HousingWealthModule(
                    house_price_exp=self._f(row, 'c3220'),
                    buy_home_conditions=self._f(row, 'c3210'),
                    cash_savings=self._f(row, 'c3250_1'),
                    life_insurance=self._f(row, 'c3250_2'),
                    pension_funds=self._f(row, 'c3250_3'),
                    mutual_funds=self._f(row, 'c3250_4'),
                    public_bonds=self._f(row, 'c3250_5'),
                    corp_bonds=self._f(row, 'c3250_6'),
                    stocks=self._f(row, 'c3250_7'),
                    business_equity=self._f(row, 'c3250_8'),
                    crypto_assets=self._f(row, 'c3251_9'),
                    precious_metals=self._f(row, 'c3251_10'),
                    other_financial_details={f"c3251_{j}": self._f(row, f"c3251_{j}") for j in range(1, 9) if self._f(row, f"c3251_{j}") is not None}
                )

                #Demographics
                occ = OccupationalProfile(
                    edu_level=row.get('x1040_1'),
                    employment_sector=row.get('x1040_2'),
                    occupation_type=row.get('x1040_3'),
                    industry=row.get('x1040_4'),
                    hours_worked=row.get('x1040_5'),
                    firm_size=row.get('x1040_6'),
                    contract_type=row.get('x1040_7'),
                    tenure=row.get('x1040_8'),
                    public_private=row.get('x1040_9')
                )

                members = []
                for m_idx in range(1, 11): 
                    v2150 = self._f(row, f'c2150_{m_idx}') 
                    v2151 = self._f(row, f'c2151_{m_idx}')
                    
                    if v2150 is not None or v2151 is not None:
                        age = v2150 if (v2150 or 0) > 7 else v2151
                        rel = v2151 if (v2150 or 0) > 7 else v2150
                        
                        members.append(HouseholdMember(
                            member_id=m_idx, 
                            age=age, 
                            relation_status=rel
                        ))

                members = []
                for m_idx in range(1, 11): 
                    age_val = self._f(row, f'c2151_{m_idx}') 
                    rel_val = self._f(row, f'c2150_{m_idx}')
                    
                    if age_val is not None or rel_val is not None:
                        members.append(HouseholdMember(
                            member_id=m_idx, 
                            age=age_val, 
                            relation_status=rel_val
                        ))

                demo = DemographicsModule(
                    gender=self._i(row, 'c1010'),
                    age_group=self._i(row, 'c1020'),
                    household_size=self._i(row, 'c2110'),
                    num_children=self._i(row, 'c2120'),
                    occupation=occ,
                    household_members=members
                )

                credit_raw = {f"debt_{j}": self._f(row, f"c8011_{j}") for j in [2, 4, 5]}
                obs = CESObservation(
                    id=i,
                    observation_date=datetime.strptime(row['Date'], '%Y-%m-%d').date(),
                    macro=macro, consumption=cons, labor=labor, 
                    housing=housing, demographics=demo,
                    credit=CreditDebtModule(
                        mortgage_access=self._f(row, 'c8010_1'),
                        consumer_credit_access=self._f(row, 'c8010_2'),
                        bank_loan_apply=self._f(row, 'c8010_3'),
                        credit_constraint_flag=self._f(row, 'c8011_1'),
                        loan_repayment_struggle=self._f(row, 'c8011_3'),
                        debt_details={k: v for k, v in credit_raw.items() if v is not None}
                    ),
                    survey_weight=self._f(row, 'wgt') or 1.0,
                    sentiment_flags={k: row[k] for k in row if any(x in k for x in ['x6020', 'h2020', 'c60', 'x8020', 'x8110'])},
                    quarterly_data={k: row[k] for k in row if k.endswith('_q')},
                    quality_audit={k: row[k] for k in row if k.endswith('_nr')}
                )
                observations.append(obs)
        return observations