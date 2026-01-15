# Eurozone-Consumer-Trend-Analyzer


**Advanced engine for Eurozone CES (Consumer Expectations Survey) data analysis.** Implements **SOLID principles**, **async processing**, **Pydantic validation**, and **16/16 unit tests**. Designed for econometric analysis of Central Bank consumer sentiment data.

## Quick Start
git clone https://github.com/JoannaJedrzejewska/Eurozone-Consumer-Trend-Analyzer <br>
cd Eurozone-Consumer-Trend-Analyzer <br>
pip install -r requirements.txt  <br>

### Test (16/16 expected)
pytest test_system.py -v

### Run
python main.py


## Features

| Feature | Description |
|---------|-------------|
| **Data Parsing** | CES CSV → Pydantic `CESObservation` (c4030 inflation, c4031 unemployment, etc.) |
| **Async Loading** | `aiofiles` + `ThreadPoolExecutor(4 workers)` |
| **Strategies** | GenericMean, WeightedMean, Percentile, DescriptiveStats, DemographicsQuality |
| **Validation** | Pydantic validators (date 2000-2030, `survey_weight > 0`) |
| **Filtering** | Date range, demographics, employment status |
| **Tests** | 100% coverage, edge cases, None handling |
| **Logging** | Structured DEBUG/INFO/ERROR logs |

##  File Structure

| File | Purpose | Key Components |
|------|---------|----------------|
| **`models.py`** | **Pydantic data models + validation** | `CESObservation`, `MacroModule`, `LaborModule`, validators (date 2000-2030, `survey_weight>0`) |
| **`gateway.py`** | **CSV data loader** | `CESDataGateway.load_all_data()` → `List[CESObservation]` |
| **`engine.py`** | **Analysis engine + 6 strategies** | `AnalyticsEngine`, `AnalysisStrategy` Protocol, 5x strategies + `filter_by_date()` |
| **`main.py`** | **Interactive CLI interface** | Menu (8 options), plotext charts, user input handling |
| **`test_system.py`** | **16 comprehensive unit tests** | Pydantic validation, strategy calculations, edge cases, gateway parsing |

## Architecture Overview

```mermaid
graph TD
    A[main.py **CLI**] --> B[engine.py **AnalyticsEngine**]
    B --> C[AnalysisStrategy Protocol]
    B --> D[GenericMeanStrategy]
    B --> E[WeightedMeanStrategy] 
    B --> F[PercentileStrategy]
    B --> G[DescriptiveStatsStrategy]
    B --> H[DemographicsQualityStrategy]
    H[gateway.py **CSV Parser**] --> B
    I[models.py **CESObservation**] -.->|used by| B
