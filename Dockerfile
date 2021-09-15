FROM python:3.8-slim-buster
ARG EnvironmentVariable
WORKDIR /app
COPY Pipfile Pipfile
COPY Pipfile.lock Pipfile.lock
RUN python3 -m pip install micropipenv[toml]
RUN micropipenv install --deploy 
COPY /app /app
CMD gunicorn -w 3 -k uvicorn.workers.UvicornWorker app:app -b 0.0.0.0:$PORT

