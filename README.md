# Eurozone-Consumer-Trend-Analyzer

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![Pydantic](https://img.shields.io/badge/Pydantic-v2-green)](https://pydantic.dev/)
[![pytest](https://img.shields.io/badge/pytest-16%2F16-brightgreen)](https://pytest.org/)
[![Async](https://img.shields.io/badge/Async-AIOFiles%20%2B%20ThreadPool-orange)](https://docs.python.org/3/library/asyncio.html)

**Advanced engine for Eurozone CES (Consumer Expectations Survey) data analysis.** Implements **SOLID principles**, **async processing**, **Pydantic validation**, and **16/16 unit tests**. Designed for econometric analysis of Central Bank consumer sentiment data.

## Features

| Feature || Description |
|---------|-------------|
| **Data Parsing** | CES CSV → Pydantic `CESObservation` (c4030 inflation, c4031 unemployment, etc.) |
| **Async Loading** | `aiofiles` + `ThreadPoolExecutor(4 workers)` |
| **Strategies** | GenericMean, WeightedMean, Percentile, DescriptiveStats, DemographicsQuality |
| **Validation** | Pydantic validators (date 2000-2030, `survey_weight > 0`) |
| **Filtering** | Date range, demographics, employment status |
| **Tests** | 100% coverage, edge cases, None handling |
| **Logging** | Structured DEBUG/INFO/ERROR logs |

## Architecture Overview

```mermaid
graph TD
    A[main.py CLI] --> B[AnalyticsEngine]
    B --> C[AnalysisStrategy Protocol]
    B --> D[GenericMeanStrategy]
    B --> E[WeightedMeanStrategy] 
    B --> F[PercentileStrategy]
    B --> G[DescriptiveStatsStrategy]
    H[gateway.py CSV] --> I[CESObservation models.py]
    I --> C
