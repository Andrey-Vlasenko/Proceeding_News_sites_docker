FROM python:3.7
WORKDIR /app
COPY requirements.txt requirements.txt
RUN python -m pip install --upgrade pip
RUN pip3 install -r requirements.txt
RUN pip3 install -U sklearn
RUN pip3 install -U nltk
RUN pip3 install -U scikit-learn
RUN pip3 install sklearn
RUN pip3 install nltk
RUN pip3 install scikit-learn
COPY . .