@echo off

SET script_path=%~dp0

CMD /K "python -m venv %script_path%.venv & %script_path%.venv\Scripts\activate.bat & pip install -r %script_path%requirements.txt & python %script_path%main.py & deactivate"
