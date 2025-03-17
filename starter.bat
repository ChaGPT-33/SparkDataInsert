@echo off
@setlocal 

rem Virtual env
cd /d "%~dp0"
set VENV_PATH="./venv"

IF EXIST %VENV_PATH% (
    echo Activating existing virtual environment
    call %VENV_PATH%\Scripts\activate.bat
) ELSE (
    echo Creating new virtual environment
    python -m venv %VENV_PATH%  
    echo Activating existing virtual environment
    call %VENV_PATH%\Scripts\activate.bat
)

rem Read requirements.txt to download necessary python library
pip install -r requirements.txt


rem execute the main function

python ./src/DMOP.py
python ./src/tax_report.py
