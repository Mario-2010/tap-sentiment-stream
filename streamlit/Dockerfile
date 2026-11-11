# Questo file di tipo Docker costruisce il container della dashboard dello Streamlit.
#Utilizza un semplice Python e installa le librerie dove poi copia il codice condiviso con Spark e scarica il modello
#che trasforma le frasi in vettori:
#serve per cercare i post simili alla domanda dell'utente.
#copia app.py e avvia la dashboard nella porta 8501

FROM python:3.11-slim
WORKDIR /app
COPY streamlit/requirements.txt .
RUN pip install --no-cache-dir --default-timeout=300 --retries 10 -r requirements.txt
COPY common/ /app/
RUN python download_models.py /models embedding
COPY streamlit/app.py /app/
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--browser.gatherUsageStats=false"]



