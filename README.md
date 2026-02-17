# 📄 Data Analysis Tool

A comprehensive Streamlit-based web application for analyzing JSON reports, extracting DIKSHA IDs from images using OCR, and comparing datasets from multiple file formats.

## 🌟 Features

### 1. **Report Analysis** 📄
- Upload and analyze JSON/TXT files containing project report data
- Automatically parse and flatten nested JSON structures
- Interactive data filtering and column selection
- Export filtered data to CSV format

**Key Capabilities:**
- **Main Result Data**: View normalized project-level information
- **Tasks Data**: Analyze task-level details with status tracking
- **Attachments Data**: Extract and review file attachments metadata
- **Advanced Filtering**: Multi-column value-based filtering
- **Column Visibility**: Keyword-based column filtering for focused analysis
- **Data Export**: Download full or filtered datasets as CSV

### 2. **DIKSHA ID Extraction** 🔍
Extract DIKSHA IDs from images using Optical Character Recognition (OCR).

**Two Processing Modes:**

#### Single Image Upload
- Upload individual images (JPG, PNG, JPEG)
- Instant OCR processing with visual preview
- Copy-friendly text output

#### Batch CSV Processing
- Process multiple images via Google Drive links
- Auto-detect Drive link columns
- Progress tracking with real-time updates
- Bulk extraction with downloadable results

**OCR Features:**
- Image preprocessing (grayscale conversion, upscaling)
- Intelligent text parsing to locate DIKSHA IDs
- Handles various Google Drive URL formats
- Error handling for invalid images/links

### 3. **Reports Comparison** ⚖️
Compare and match data between two files (CSV, JSON, or TXT).

**Comparison Features:**
- Flexible file format support (CSV, JSON, TXT)
- Custom key column selection for matching
- Automatic data normalization
- Separate matched and unmatched datasets
- Visual match statistics with progress indicators
- Export matched/unmatched results

## 📋 Requirements

### Python Dependencies
Install via `requirement.txt`:
```bash
pip install -r requirement.txt
```

**Required packages:**
- `streamlit` - Web application framework
- `pandas` - Data manipulation and analysis
- `Pillow` - Image processing
- `pytesseract` - OCR engine wrapper
- `gdown` - Google Drive file downloader

### System Dependencies
Install via `packages.txt` (for Streamlit Cloud deployment):
- `tesseract-ocr` - OCR engine

**Local Installation (Ubuntu/Debian):**
```bash
sudo apt-get update
sudo apt-get install tesseract-ocr
```

**Local Installation (macOS):**
```bash
brew install tesseract
```

**Local Installation (Windows):**
Download and install from: https://github.com/UB-Mannheim/tesseract/wiki

## 🚀 Getting Started

### Installation

1. **Clone or download the project**
   ```bash
   cd /path/to/streamlit
   ```

2. **Install Python dependencies**
   ```bash
   pip install -r requirement.txt
   ```

3. **Install Tesseract OCR** (see System Dependencies above)

4. **Verify installation**
   ```bash
   tesseract --version
   ```

### Running the Application

**Local Development:**
```bash
streamlit run app.py
```

The application will open in your default browser at `http://localhost:8501`

**Streamlit Cloud Deployment:**
1. Push your code to GitHub
2. Connect your repository to [Streamlit Cloud](https://streamlit.io/cloud)
3. The `packages.txt` file will automatically install system dependencies

## 📖 Usage Guide

### Tab 1: Report Analysis

1. **Upload File**: Click "Upload JSON / TXT file" and select your report file
2. **Filter Rows**: 
   - Select columns to filter
   - Enter search values for each selected column
3. **Select Columns**: 
   - Use keyword search to find specific columns
   - Choose which columns to display
4. **Download Data**: 
   - Download full dataset or filtered results
   - Available for Main, Tasks, and Attachments data

### Tab 2: DIKSHA ID Extraction

#### Single Image Mode
1. Select "Single Image Upload"
2. Upload an image containing a DIKSHA ID
3. Click "Extract ID"
4. Copy the extracted ID from the code block

#### Batch Processing Mode
1. Select "Batch CSV Processing"
2. Upload a CSV file with Google Drive links
3. Select the column containing Drive links
4. Click "Start Batch Processing"
5. Wait for processing to complete
6. Download the results CSV with extracted IDs

**CSV Format Example:**
```csv
Name,Drive Link,Other Data
User1,https://drive.google.com/file/d/FILE_ID/view,Data1
User2,https://drive.google.com/file/d/FILE_ID/view,Data2
```

### Tab 3: Reports Comparison

1. **Upload Base File**: The dataset you want to filter
2. **Upload Reference File**: The dataset containing filter values
3. **Select Key Columns**: Choose matching columns from both files
4. **Click "Find Matches"**: Process the comparison
5. **Download Results**: 
   - Matched rows (records found in both files)
   - Unmatched rows (records only in base file)

**Use Cases:**
- Filter user reports by a list of user IDs
- Find projects matching specific criteria
- Identify missing or extra records between datasets

## 🗂️ Project Structure

```
streamlit/
├── app.py              # Main application file
├── requirement.txt     # Python dependencies
├── packages.txt        # System dependencies (for Streamlit Cloud)
└── README.md          # This file
```

## 🔧 Configuration

### OCR Configuration
The OCR engine is configured with:
- **Character whitelist**: Alphanumeric + underscore
- **Page segmentation mode**: 6 (uniform text block)
- **Preprocessing**: Grayscale + 2x upscaling

Modify in `app.py` line 216:
```python
config = r'-c tessedit_char_whitelist=... --psm 6'
```

### Streamlit Page Configuration
Customize in `app.py` line 19:
```python
st.set_page_config(page_title="Data Analysis Tool", layout="wide")
```

## 🐛 Troubleshooting

### OCR Dependencies Missing
**Error**: "Missing dependencies! Please wait if installation is in progress..."

**Solution**:
```bash
pip install pytesseract gdown
sudo apt-get install tesseract-ocr  # Linux
brew install tesseract              # macOS
```

### Google Drive Download Fails
**Common Issues**:
- File not publicly accessible → Share file with "Anyone with the link"
- Invalid URL format → Use standard Drive sharing links
- Rate limiting → Wait and retry

### JSON Parsing Errors
**Error**: "JSON must contain a 'result' key with list data"

**Solution**: Ensure your JSON follows the expected structure:
```json
{
  "result": [ /* array of objects */ ]
}
```

## 📊 Data Privacy

- All processing happens locally or in your Streamlit Cloud instance
- No data is sent to external services except:
  - Google Drive (for image downloads in batch mode)
  - Tesseract OCR (local processing)
- Temporary files are automatically cleaned up after processing

## 🤝 Contributing

To extend this application:

1. **Add new tabs**: Modify the `st.tabs()` call on line 23
2. **Customize filters**: Edit helper functions (lines 40-83)
3. **Enhance OCR**: Adjust preprocessing in `preprocess_image()` (lines 208-211)
4. **Support new formats**: Extend `load_file_to_df()` (lines 317-362)

## 📝 License

This project is provided as-is for educational and internal use.

## 💡 Tips

- **Large Files**: For files >200MB, consider processing in chunks
- **OCR Accuracy**: Use high-resolution images for best results
- **Performance**: Batch processing time depends on number of images and network speed
- **Column Names**: Use descriptive column names in CSVs for easier selection

## 🔗 Resources

- [Streamlit Documentation](https://docs.streamlit.io)
- [Tesseract OCR](https://github.com/tesseract-ocr/tesseract)
- [Pandas Documentation](https://pandas.pydata.org/docs/)

---

**Version**: 1.0  
**Last Updated**: February 2026  
**Maintained by**: Thippeswamy
