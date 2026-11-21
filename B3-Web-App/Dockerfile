FROM node:22-alpine AS frontend-build

WORKDIR /frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM python:3.15-slim

WORKDIR /app

COPY backend/ ./backend/
RUN pip install --no-cache-dir ./backend \
    && useradd --system --create-home app

COPY --from=frontend-build /frontend/dist/b3-web-app/browser ./backend/static

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

USER app

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "warning", "--no-access-log"]
