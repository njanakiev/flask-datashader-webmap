FROM continuumio/miniconda3:4.7.12

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
