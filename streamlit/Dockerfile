FROM python:3.11-slim
WORKDIR /app
COPY streamlit/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ /app/
RUN python download_models.py /models embedding
COPY streamlit/app.py /app/
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--browser.gatherUsageStats=false"]
