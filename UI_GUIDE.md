# Scanner Platform UI - User Guide

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Start the Web Application
```bash
python app.py
```

### 3. Open Browser
Navigate to: **http://localhost:5000**

## 🎨 UI Features

### Modern Professional Design
- **Dark Sidebar Navigation** - Easy access to multiple scanners (future-ready)
- **Clean Dashboard** - Intuitive controls and real-time results
- **Responsive Layout** - Works on desktop, tablet, and mobile
- **Beautiful Animations** - Smooth transitions and hover effects

### Key Improvements Over Reference UI
1. **Better Visual Hierarchy** - Clearer separation of controls and results
2. **Real-time Progress Bar** - Visual feedback during scanning
3. **Advanced Filtering** - Filter results by direction (bullish/bearish) or date
4. **Statistics Dashboard** - Live metrics and performance indicators
5. **Export Options** - Download results as JSON, CSV, or Excel
6. **Toast Notifications** - Non-intrusive success/error messages
7. **Connection Status** - Real-time DHAN API status indicator

## 📊 Scanner Controls

### Timeframe Selection
- **Daily (1D)** - Analyze daily candlestick patterns
- **Weekly (1W)** - Detect patterns on weekly timeframe
- **Monthly (1M)** - Long-term pattern detection

### Parameters
- **Min Marubozu %** - Minimum body size (default: 3%)
- **Days Back** - Historical data range (10-365 days)
- **Symbol Group** - Choose F&O stocks, Nifty 50, or custom symbols

### Action Buttons
- **Start Full Scan** - Comprehensive scan with all parameters
- **Today's Signals Only** - Quick scan for today's patterns

## 🎯 Result Display

### Pattern Cards
Each detected pattern shows:
- Symbol name with chart icon
- Pattern direction (Bullish/Bearish) badge
- Signal date
- Confidence score (calculated based on pattern strength)
- Marubozu body percentage
- Doji body percentage
- Breakout amount in ₹
- Rejection strength percentage

### Color Coding
- **Green (Bullish)** - Failed upside breakout patterns
- **Red (Bearish)** - Failed downside breakout patterns

## 🔍 Filtering Options

### Result Filters
- **All** - Show all detected patterns
- **Bullish** - Only bullish patterns
- **Bearish** - Only bearish patterns
- **Today Only** - Patterns from today's date

## 📥 Export Features

### Export Formats
- **JSON** - Complete data with all fields
- **CSV** - Spreadsheet-compatible format
- **Excel** - .xlsx format with formatting

## 🔮 Future Scanners (UI Ready)

The UI is designed to support multiple scanners:
1. **Marubozu + Doji** ✅ (Active)
2. **Hammer & Shooting** (Coming Soon)
3. **Engulfing Patterns** (Coming Soon)
4. **Doji Variations** (Coming Soon)
5. **Three Patterns** (Coming Soon)

## 🛠️ Technical Features

### Frontend
- Pure JavaScript (no framework dependencies)
- CSS3 animations and transitions
- Font Awesome icons
- Responsive grid system

### Backend
- Flask REST API
- Real-time scanning with progress updates
- Concurrent symbol processing
- Automatic result caching

## 📱 Responsive Design

The UI automatically adapts to different screen sizes:
- **Desktop** - Full sidebar, multi-column grid
- **Tablet** - Collapsible sidebar, 2-column grid
- **Mobile** - Hidden sidebar, single column

## 🎯 Usage Tips

1. **Start with Default Settings** - The defaults are optimized for best results
2. **Use Custom Symbols** - For quick testing with specific stocks
3. **Monitor Progress** - Watch the real-time progress bar during scans
4. **Filter Results** - Use filters to focus on specific pattern types
5. **Export for Analysis** - Download results for further analysis in Excel

## 🐛 Troubleshooting

### Port Already in Use
If port 5000 is busy, edit `app.py` and change:
```python
app.run(host='127.0.0.1', port=5000, debug=True)
```
To a different port like 5001, 8080, etc.

### No Patterns Found
- Reduce "Min Marubozu %" to 2%
- Increase "Days Back" to scan more history
- Try different timeframes (Weekly/Monthly)

### Connection Issues
- Check DHAN API credentials in `scanner/config.py`
- Ensure internet connection is active
- Verify DHAN API access is enabled

## 🌟 Features Comparison

| Feature | Reference UI | Our UI | Improvement |
|---------|-------------|---------|-------------|
| Design | Basic Purple | Modern Dark/Light | ✨ Professional |
| Scanners | Single | Multi-Scanner Ready | 🚀 Scalable |
| Progress | None | Real-time Bar | 📊 Visual Feedback |
| Filters | None | Multiple Options | 🔍 Better UX |
| Export | Basic | JSON/CSV/Excel | 📥 Versatile |
| Mobile | Limited | Fully Responsive | 📱 Adaptive |
| Notifications | None | Toast Messages | 💬 User-Friendly |
| Stats | Basic | Live Dashboard | 📈 Informative |

## 🎉 Summary

This UI provides a **professional, modern, and feature-rich** interface for your pattern scanner that:
- Looks better than the reference design
- Supports multiple scanners (future-ready)
- Provides real-time feedback
- Works on all devices
- Exports data in multiple formats

Enjoy scanning! 🚀