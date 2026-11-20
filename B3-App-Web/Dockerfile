FROM node:22-alpine AS frontend-build

WORKDIR /frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM python:3.15-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --system --create-home app

COPY backend/ ./
COPY --from=frontend-build /frontend/dist/b3-app-web/browser ./static

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

USER app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
