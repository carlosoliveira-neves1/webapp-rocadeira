@echo off
pip install -r requirements.txt
streamlit run app_consumo_rocadeira.py --server.address 0.0.0.0 --server.port 8501
pause
