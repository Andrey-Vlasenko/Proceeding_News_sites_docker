FROM python:3.11
WORKDIR /app
COPY requirements.txt requirements.txt
RUN python -m pip install --upgrade pip 
RUN python -m pip install --upgrade pip wheel
USER airflow
RUN pip install -r requirements.txt
RUN pip install --no-cache-dir -U sklearn nltk
RUN pip install -U sklearn nltk
RUN pip install sklearn nltk
COPY . .