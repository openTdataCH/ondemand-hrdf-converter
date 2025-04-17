FROM python:3-alpine
ADD main.py .
ADD requirements.txt .
RUN pip install -r ./requirements.txt
RUN mkdir "output"
ENTRYPOINT ["python","./main.py"]