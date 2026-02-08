# Industrial Ingestion Pipeline - Windows Demo Setup

Write-Host "Industrial Ingestion Pipeline - Demo Setup" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Date: $(Get-Date)" -ForegroundColor Gray
Write-Host ""

# Create necessary directories
Write-Host "Creating directories..." -ForegroundColor Blue
New-Item -ItemType Directory -Force -Path logs | Out-Null
New-Item -ItemType Directory -Force -Path data | Out-Null
Write-Host "Directories created" -ForegroundColor Green
Write-Host ""

# Create virtual environment
Write-Host "Creating Python virtual environment..." -ForegroundColor Blue
try {
    python -m venv venv
    Write-Host "Virtual environment created" -ForegroundColor Green
}
catch {
    Write-Host "Could not create virtual environment, using system Python" -ForegroundColor Yellow
}
Write-Host ""

# Activate virtual environment and install dependencies
Write-Host "Installing Python dependencies..." -ForegroundColor Blue
if (Test-Path "venv\Scripts\Activate.ps1") {
    & venv\Scripts\Activate.ps1
}

python -m pip install --upgrade pip
python -m pip install -r requirements.txt
Write-Host "Dependencies installed" -ForegroundColor Green
Write-Host ""

# Create environment file template
Write-Host "Creating environment configuration..." -ForegroundColor Blue
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env file from template" -ForegroundColor Green
    Write-Host "IMPORTANT: Edit .env file with your Gmail credentials:" -ForegroundColor Yellow
    Write-Host "   - IMAP_USERNAME: Your Gmail address" -ForegroundColor Gray
    Write-Host "   - IMAP_PASSWORD: Your Gmail App Password" -ForegroundColor Gray
    Write-Host "   - Generate App Password: https://myaccount.google.com/apppasswords" -ForegroundColor Gray
}
else {
    Write-Host ".env file already exists" -ForegroundColor Green
}
Write-Host ""

# Initialize database
Write-Host "Initializing database..." -ForegroundColor Blue
if (Test-Path "venv\Scripts\Activate.ps1") {
    & venv\Scripts\Activate.ps1
}

python init_database.py
Write-Host ""

# Test the setup
Write-Host "Testing the setup..." -ForegroundColor Blue
if (Test-Path "venv\Scripts\Activate.ps1") {
    & venv\Scripts\Activate.ps1
}

python test_setup.py
Write-Host ""

# Start the demo
Write-Host "Starting the demo..." -ForegroundColor Green
Write-Host ""
Write-Host "Demo Instructions:" -ForegroundColor Cyan
Write-Host "1. Send CSV files to jmashoana@gmail.com" -ForegroundColor Gray
Write-Host "2. Watch the dashboard for real-time processing" -ForegroundColor Gray
Write-Host "3. Observe file processing and metrics" -ForegroundColor Gray
Write-Host ""
Write-Host "Test files available in data directory:" -ForegroundColor Cyan
Write-Host "   - test_batch_01_high_success.csv" -ForegroundColor Gray
Write-Host "   - test_batch_02_mixed_results.csv" -ForegroundColor Gray
Write-Host "   - test_batch_03_high_failure.csv" -ForegroundColor Gray
Write-Host "   - test_batch_04_edge_cases.csv" -ForegroundColor Gray
Write-Host ""
Write-Host "Dashboard will start in 3 seconds..." -ForegroundColor Yellow
Start-Sleep -Seconds 3

if (Test-Path "venv\Scripts\Activate.ps1") {
    & venv\Scripts\Activate.ps1
}

python start_demo.py
