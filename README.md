# Fashion Studio ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for extracting competitor fashion product data from Dicoding's Fashion Studio website. This project implements modular design principles with extensive unit testing and data quality validation.

## 🚀 Features

- **Modular Architecture**: Separate modules for extract, transform, and load operations
- **Pagination Support**: Extract data from multiple pages (1-50 pages, up to 1000 products)
- **Currency Conversion**: Automatic USD to IDR conversion (rate: 16,000)
- **Data Quality**: Comprehensive data cleaning, validation, and duplicate removal
- **Timestamp Tracking**: Data lineage with extraction timestamps
- **Comprehensive Testing**: 33 unit tests covering all modules
- **Flexible Output**: Support for both overwrite and append modes
- **Backup Functionality**: Automatic backup creation for data safety

## 📊 Data Schema

The pipeline extracts and transforms the following data fields:

| Field | Type | Description |
|-------|------|-------------|
| `title` | string | Product name |
| `price` | float | Product price in IDR (converted from USD) |
| `rating` | float | Product rating (1-5 scale) |
| `colors` | integer | Number of available colors |
| `size` | string | Product size (S, M, L, XL, XXL) |
| `gender` | string | Target gender (Men, Women, Unisex) |
| `timestamp` | string | Data extraction timestamp |

## 🏗️ Project Structure

```
submission-pemda/
├── tests/
│   ├── test_extract.py     # Unit tests for extraction module
│   ├── test_transform.py   # Unit tests for transformation module
│   └── test_load.py        # Unit tests for loading module
├── utils/
│   ├── extract.py          # Data extraction module
│   ├── transform.py        # Data transformation module
│   └── load.py             # Data loading module
├── main.py                 # Main ETL pipeline orchestrator
├── requirements.txt        # Python dependencies
├── README.md              # This documentation
├── .gitignore             # Git ignore rules
└── products.csv           # Output data file (generated after running)
```

## 🛠️ Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd submission-pemda
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## 🚀 Usage

### Basic Usage
```bash
python main.py
```

### Extract from Multiple Pages
```bash
python main.py --max-pages 50
```

### Custom Configuration
```bash
python main.py --url https://fashion-studio.dicoding.dev/ --output custom_products.csv --mode append --max-pages 10
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--url` | Source website URL | https://fashion-studio.dicoding.dev/ |
| `--output` | Output CSV filename | products.csv |
| `--mode` | Save mode (overwrite/append) | overwrite |
| `--max-pages` | Maximum pages to scrape | 5 |

## 🧪 Testing

Run the complete test suite:
```bash
pytest tests/ -v
```

Run specific test modules:
```bash
pytest tests/test_extract.py -v
pytest tests/test_transform.py -v
pytest tests/test_load.py -v
```

### Test Coverage
- **Extract Module**: 8 test cases
- **Transform Module**: 13 test cases
- **Load Module**: 12 test cases
- **Total**: 33 test cases

## 📈 Pipeline Performance

- **Data Source**: Fashion Studio website with pagination
- **Extraction Capacity**: Up to 1000 products from 50 pages
- **Data Quality**: Strict validation removes invalid/duplicate records
- **Typical Output**: 15-20 valid products after quality filtering
- **Processing Speed**: ~2-3 seconds per page

## 🔧 Technical Implementation

### Technologies Used
- **Web Scraping**: `requests` + `BeautifulSoup4`
- **Data Processing**: `pandas`
- **Testing**: `pytest`
- **Data Format**: CSV

### Data Quality Measures
- ✅ Null value removal
- ✅ Duplicate detection and removal
- ✅ Data type validation
- ✅ Format standardization
- ✅ Currency conversion validation
- ✅ Rating range validation (1-5)

### Error Handling
- Network timeout handling
- Invalid URL detection
- Malformed HTML parsing
- File I/O error management
- Data validation failures

## 📝 Example Output

```csv
title,price,rating,colors,size,gender,timestamp
T-shirt 2,1634400.0,3.9,3,M,Women,2025-06-15 12:48:14
Hoodie 3,7950080.0,4.8,3,L,Unisex,2025-06-15 12:48:14
Pants 4,7476960.0,3.3,3,XL,Men,2025-06-15 12:48:14
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📋 Requirements

- Python 3.7+
- Internet connection for web scraping
- Dependencies listed in `requirements.txt`

## 📄 License

This project is created for educational purposes as part of a data engineering assignment.

## 🔗 Related Links

- [Fashion Studio Website](https://fashion-studio.dicoding.dev/)

---

**Note**: This ETL pipeline is designed for educational and research purposes. Please ensure compliance with the website's robots.txt and terms of service when using this tool.
