FROM python:3.8

ENV APP_NAME=thor-wrapper.py

COPY "$APP_NAME" /code/"$APP_NAME"
COPY thor-wrapper.conf /code/thor-wrapper.conf
COPY thor /code/thor
COPY loader /code/loader

WORKDIR /code

ENV FLASK_APP="$APP_NAME"
ENV FLASK_RUN_HOST=0.0.0.0

RUN pip3 install --no-cache-dir pip==22.1.1

COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 5000

CMD ["flask", "run"]
