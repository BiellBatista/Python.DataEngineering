"""Logging estruturado, traces e métricas do B3 Data Explorer."""

import json
import logging
import os
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, Request
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Span, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode
from starlette.middleware.base import RequestResponseEndpoint
from starlette.responses import Response


class JsonLogFormatter(logging.Formatter):
    """Formata mensagens e metadados para coleta no Loki."""

    _FIELDS = (
        "request_id",
        "http_method",
        "http_path",
        "http_status",
        "duration_ms",
        "frontend_event",
        "catalog_tables",
        "result_rows",
        "error_type",
        "outcome",
    )

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "timestamp": datetime.now().astimezone().isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in self._FIELDS:
            if hasattr(record, field):
                entry[field] = getattr(record, field)

        span_context = trace.get_current_span().get_span_context()
        if span_context.is_valid:
            entry["trace_id"] = f"{span_context.trace_id:032x}"
            entry["span_id"] = f"{span_context.span_id:016x}"
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(entry, ensure_ascii=False, default=str)


def _configure_logging() -> logging.Logger:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, None)
    if not isinstance(level, int):
        raise ValueError(f"LOG_LEVEL inválido: {level_name}")

    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level)
    return logging.getLogger("b3_app_web")


logger = _configure_logging()


def _configure_telemetry() -> tuple[TracerProvider | None, MeterProvider | None]:
    enabled_value = os.getenv("OTEL_ENABLED", "false").lower()
    if enabled_value not in {"true", "false"}:
        raise ValueError("OTEL_ENABLED deve ser true ou false.")
    if enabled_value == "false":
        return None, None

    export_interval = int(os.getenv("OTEL_METRIC_EXPORT_INTERVAL", "15000"))
    if export_interval <= 0:
        raise ValueError("OTEL_METRIC_EXPORT_INTERVAL deve ser maior que zero.")

    resource = Resource.create(
        {
            "service.name": os.getenv("OTEL_SERVICE_NAME", "b3-web-app-api"),
            "service.version": "0.1.0",
            "deployment.environment.name": os.getenv(
                "DEPLOYMENT_ENVIRONMENT",
                "local",
            ),
        }
    )
    timeout = float(os.getenv("OTEL_EXPORTER_OTLP_TIMEOUT", "5"))
    base_endpoint = os.getenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://otel-lgtm:4318",
    ).rstrip("/")
    traces_endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") or (
        f"{base_endpoint}/v1/traces"
    )
    metrics_endpoint = os.getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") or (
        f"{base_endpoint}/v1/metrics"
    )

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=traces_endpoint, timeout=timeout)
        )
    )
    trace.set_tracer_provider(tracer_provider)

    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=metrics_endpoint, timeout=timeout),
        export_interval_millis=export_interval,
    )
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[metric_reader],
    )
    metrics.set_meter_provider(meter_provider)
    return tracer_provider, meter_provider


tracer_provider, meter_provider = _configure_telemetry()
tracer = trace.get_tracer("b3_app_web", "0.1.0")
meter = metrics.get_meter("b3_app_web", "0.1.0")

HTTP_REQUEST_COUNT = meter.create_counter(
    "b3_app_web.http.server.requests",
    unit="{request}",
    description="Requisições HTTP atendidas pela API.",
)
HTTP_REQUEST_DURATION = meter.create_histogram(
    "b3_app_web.http.server.duration",
    unit="s",
    description="Duração das requisições HTTP da API.",
)
QUERY_COUNT = meter.create_counter(
    "b3_app_web.query.count",
    unit="{query}",
    description="Consultas SQL aceitas, rejeitadas ou com falha.",
)
QUERY_DURATION = meter.create_histogram(
    "b3_app_web.query.duration",
    unit="s",
    description="Duração total das consultas SQL.",
)
FRONTEND_EVENT_COUNT = meter.create_counter(
    "b3_app_web.frontend.event.count",
    unit="{event}",
    description="Eventos de observabilidade emitidos pelo frontend.",
)
FRONTEND_ACTION_DURATION = meter.create_histogram(
    "b3_app_web.frontend.action.duration",
    unit="s",
    description="Duração percebida das ações do frontend.",
)


def record_query_metrics(outcome: str, duration_seconds: float) -> None:
    attributes = {"outcome": outcome}
    QUERY_COUNT.add(1, attributes)
    QUERY_DURATION.record(duration_seconds, attributes)


def record_frontend_event(event: str, duration_ms: float | None) -> None:
    attributes = {"event": event}
    FRONTEND_EVENT_COUNT.add(1, attributes)
    if duration_ms is not None:
        FRONTEND_ACTION_DURATION.record(duration_ms / 1000, attributes)


def _set_error_status(span: Span, description: str) -> None:
    span.set_status(Status(StatusCode.ERROR, description))


def _record_http_metrics(
    method: str,
    path: str,
    status_code: int,
    duration_seconds: float,
) -> None:
    attributes = {
        "http.request.method": method,
        "http.route": path,
        "http.response.status_code": status_code,
    }
    HTTP_REQUEST_COUNT.add(1, attributes)
    HTTP_REQUEST_DURATION.record(duration_seconds, attributes)


async def observe_http_requests(
    request: Request,
    call_next: RequestResponseEndpoint,
) -> Response:
    path = request.url.path
    if not path.startswith("/api/"):
        return await call_next(request)

    if path == "/api/health":
        with tracer.start_as_current_span(
            "http.server.health",
            kind=SpanKind.SERVER,
            attributes={
                "http.request.method": request.method,
                "url.path": path,
            },
        ) as span:
            response = await call_next(request)
            span.set_attribute("http.response.status_code", response.status_code)
            if response.status_code >= 500:
                _set_error_status(span, "A verificação de saúde falhou.")
            return response

    request_id = uuid4().hex
    started_at = time.perf_counter()
    span_attributes = {
        "http.request.method": request.method,
        "url.path": path,
    }

    with tracer.start_as_current_span(
        "http.server.request",
        kind=SpanKind.SERVER,
        attributes=span_attributes,
    ) as span:
        try:
            response = await call_next(request)
        except Exception as error:
            duration_seconds = time.perf_counter() - started_at
            _set_error_status(span, "Falha interna ao processar a requisição.")
            span.set_attribute("http.response.status_code", 500)
            _record_http_metrics(
                request.method,
                path,
                500,
                duration_seconds,
            )
            logger.error(
                "Falha inesperada ao processar a requisição HTTP.",
                extra={
                    "request_id": request_id,
                    "http_method": request.method,
                    "http_path": path,
                    "http_status": 500,
                    "duration_ms": round(duration_seconds * 1000, 2),
                    "error_type": type(error).__name__,
                },
            )
            raise

        duration_seconds = time.perf_counter() - started_at
        status_code = response.status_code
        span.set_attribute("http.response.status_code", status_code)
        if status_code >= 500:
            _set_error_status(span, "A API retornou um erro interno.")
        _record_http_metrics(
            request.method,
            path,
            status_code,
            duration_seconds,
        )
        response.headers["X-Request-ID"] = request_id

        log_level = logging.WARNING if status_code >= 400 else logging.INFO
        message = (
            "Requisição HTTP concluída."
            if status_code < 400
            else "Requisição HTTP concluída com erro."
        )
        logger.log(
            log_level,
            message,
            extra={
                "request_id": request_id,
                "http_method": request.method,
                "http_path": path,
                "http_status": status_code,
                "duration_ms": round(duration_seconds * 1000, 2),
            },
        )
        return response


@asynccontextmanager
async def observability_lifespan(_: FastAPI) -> AsyncIterator[None]:
    logger.info("API do Data Explorer iniciada.")
    try:
        yield
    finally:
        if meter_provider is not None:
            if not meter_provider.force_flush(timeout_millis=5000):
                logger.warning("O envio final das métricas excedeu o tempo limite.")
            meter_provider.shutdown()
        if tracer_provider is not None:
            if not tracer_provider.force_flush(timeout_millis=5000):
                logger.warning("O envio final dos traces excedeu o tempo limite.")
            tracer_provider.shutdown()
        logger.info("API do Data Explorer encerrada.")
