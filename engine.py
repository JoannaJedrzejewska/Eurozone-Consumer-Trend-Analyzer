from concurrent.futures import ThreadPoolExecutor
from typing import List, Protocol, runtime_checkable, Optional
from collections import defaultdict
import asyncio
from models import CESObservation

@runtime_checkable
class AnalysisStrategy(Protocol):
    def compute(self, data: List[CESObservation]) -> float: ...

class GenericMeanStrategy:
    def __init__(self, attr_path: str):
        self.attr_path = attr_path

    def compute(self, data: List[CESObservation]) -> float:
        values = []
        for obs in data:
            val = None
            try:
                temp_val = obs
                for part in self.attr_path.split('.'):
                    temp_val = getattr(temp_val, part)
                val = temp_val
            except (AttributeError, TypeError):
                val = obs.sentiment_flags.get(self.attr_path)
                
                if val is None:
                    val = obs.sentiment_flags.get(f"{self.attr_path}_1")

            if isinstance(val, (int, float)):
                values.append(val)
        
        return sum(values) / len(values) if values else 0.0
class DemographicsQualityStrategy:
    """Strategy to calculate the percentage of missing values (N/As) in demographics."""
    def compute(self, data: List[CESObservation]) -> dict[str, float]:
        total_records = len(data)
        missing_counts = {
            "Gender": 0,
            "Age Group": 0,
            "Household Size": 0,
            "Education Info": 0,
            "Household Members": 0
        }

        for obs in data:
            if obs.demographics.gender is None: missing_counts["Gender"] += 1
            if obs.demographics.age_group is None: missing_counts["Age Group"] += 1
            if obs.demographics.household_size is None: missing_counts["Household Size"] += 1
            if not obs.demographics.occupation.edu_level: missing_counts["Education Info"] += 1
            if not obs.demographics.household_members: missing_counts["Household Members"] += 1

        return {k: (v / total_records) * 100 for k, v in missing_counts.items()}

class AnalyticsEngine:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=4)

    def get_available_years(self, data: List[CESObservation]) -> List[int]:
        return sorted(list(set(obj.observation_date.year for obj in data)))

    def filter_by_date(self, data: List[CESObservation], start_year: int, end_year: int) -> List[CESObservation]:
        return [obj for obj in data if start_year <= obj.observation_date.year <= end_year]

    async def run_analysis(self, data: List[CESObservation], strategy: AnalysisStrategy) -> float:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, strategy.compute, data)

    async def get_yearly_report(self, data: List[CESObservation]) -> dict[int, float]:
        grouped = defaultdict(list)
        for obs in data:
            grouped[obs.observation_date.year].append(obs)
        
        report = {}
        strategy = GenericMeanStrategy("macro.inflation_1y")
        for year, observations in grouped.items():
            report[year] = await self.run_analysis(observations, strategy)
        return report
    def find_by_date(self, data: list[CESObservation], search_str: str) -> Optional[CESObservation]:
        """
        Searches the collection for a specific month/year.
        Supports: '2023-03', '03-2023', 'March 2023'
        """
        search_str = search_str.lower().strip()
        
        # Month name mapping
        months = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
        }

        for obs in data:
            d = obs.observation_date
            if search_str == d.strftime("%Y-%m"): return obs
            if search_str == d.strftime("%m-%Y"): return obs
            if any(m_name in search_str for m_name in months) and str(d.year) in search_str:
                m_name = next(k for k in months if k in search_str)
                if months[m_name] == d.month:
                    return obs
                    
        return None

    async def run_quality_report(self, data: List[CESObservation]) -> dict[str, float]:
        """Calculates demographics completeness using multithreading."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, DemographicsQualityStrategy().compute, data)

    async def get_time_series(self, data: List[CESObservation], attr_path: str) -> tuple[List[str], List[float]]:
        loop = asyncio.get_running_loop()
        
        def process():
            date_groups = defaultdict(list)
            for obs in data:
                date_groups[obs.observation_date].append(obs)
            
            sorted_dates = sorted(date_groups.keys())
            x_axis, y_axis = [], []
            strategy = GenericMeanStrategy(attr_path)
            
            for d in sorted_dates:
                mean_val = strategy.compute(date_groups[d])
                x_axis.append(d.strftime("%Y-%m"))
                y_axis.append(mean_val)
            return x_axis, y_axis

        return await loop.run_in_executor(self.executor, process)