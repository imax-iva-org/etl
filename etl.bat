@echo on

cd C:\Users\max\PycharmProjects\etl\venv\Scripts
call activate.bat

cd C:\Users\max\PycharmProjects\etl\etl
python fb.py

cd C:\Users\max\PycharmProjects\etl\venv\Scripts
call deactivate.bat
