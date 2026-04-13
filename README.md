# HICP Weights Downloader

Download **Harmonised Index of Consumer Prices (HICP) item weights** from Eurostat using the [ECOICOP v2 classification](https://ec.europa.eu/eurostat/databrowser/view/prc_hicp_iw/default/table?lang=en) and save them as a Parquet file.

## What it does

This project downloads annual HICP item weights for all ECOICOP v2 categories (from the total index down to the most granular level available) for:
- **EU** and **EA** aggregates
- All **EU-27 member states**: BE, BG, CZ, DK, DE, EE, IE, EL, ES, FR, HR, IT, CY, LV, LT, LU, HU, MT, NL, AT, PL, PT, RO, SI, SK, FI, SE

Weights are expressed in **‰** (per mille, summing to 1000 for each country/year).

## Dataset details

| Field | Description |
|-------|-------------|
| `coicop18` | ECOICOP v2 classification code (e.g. `TOTAL`, `CP01`, `CP0111A`) |
| `name` | Human-readable description of the category |
| `geo` | Geographic code (e.g. `EU`, `EA`, `IT`, `DE`) |
| `time` | Reference year (e.g. `2024`) |
| `value` | Item weight in ‰ (per mille) |

**Source dataset**: [prc_hicp_iw](https://ec.europa.eu/eurostat/databrowser/view/prc_hicp_iw/default/table?lang=en) on Eurostat

## Project structure

```
├── coicop18.csv               # ECOICOP v2 classification map (code, name, hierarchy level, parent)
├── download_hicp_weights.py   # Main download script
├── README.md
└── output/                    # Generated output
    └── hicp_weights.parquet   # Downloaded weights data
```

## Requirements

- Python 3.10+
- Dependencies: `requests`, `pandas`, `pyarrow`, `tenacity`

## Installation

```bash
pip install requests pandas pyarrow tenacity
```

## Usage

```bash
python download_hicp_weights.py
```

The script will:
1. Load all ECOICOP v2 categories from `coicop18.csv`
2. Download item weights for each category from the Eurostat API
3. Combine all data and save to `output/hicp_weights.parquet`

### Example: read the parquet file

```python
import pandas as pd

df = pd.read_parquet("output/hicp_weights.parquet")

# Filter for Italy, 2024, top-level divisions
italy_2024 = df[(df["geo"] == "IT") & (df["time"] == "2024")]
print(italy_2024)
```

## License

This project is provided as-is. The downloaded data is subject to [Eurostat's copyright policy](https://ec.europa.eu/eurostat/about-us/policies/copyright).

## Inspiration

Adapted from [inflation_web/data_download.py](https://github.com/paluigi/inflation_web/blob/main/data_download.py).
