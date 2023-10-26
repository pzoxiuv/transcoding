FROM python:3

WORKDIR /code

RUN apt-get update && apt-get install -y ffmpeg

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY . /code

CMD ["python", "main.py"]
