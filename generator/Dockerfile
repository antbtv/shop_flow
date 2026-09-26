FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt /app/generator/requirements.txt
RUN pip install --no-cache-dir -r /app/generator/requirements.txt
COPY . /app/generator
USER nobody
ENTRYPOINT ["python", "-m", "generator.generate_orders"]
