FROM python:3.10.12-alpine3.17

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app


RUN pip install --upgrade pip
RUN pip install -r requirements.txt


COPY . /app

EXPOSE 5000

CMD ["python", "/app/main.py"]