FROM continuumio/miniconda3:4.7.12

RUN apt update -qq \
 && apt install -y gcc --no-install-recommends \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY environment.yml /app/environment.yml

RUN conda env update --file /app/environment.yml

COPY app /app

EXPOSE 8000

CMD ["gunicorn", \
       "-b", "0.0.0.0:8000", \
       "--log-level=debug", \
       "--timeout=200", \
       "--workers=1", \
       "app:app"]
