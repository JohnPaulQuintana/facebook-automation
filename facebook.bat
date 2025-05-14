@echo off
cd /d "%~dp0"

:: Check if env exists, if not, create it
if not exist env (
    echo Creating virtual environment...
    python -m venv env
)

:: Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat

:: Install requirements
echo Installing dependencies...
pip install -r requirements.txt


:: Run main.py
echo Running main.py...
python main.py

:: Deactivate virtual environment
echo Deactivating virtual environment...
deactivate

:: Script will now exit, which closes the terminal window
