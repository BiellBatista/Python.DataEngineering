import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import {
  AfterViewInit,
  Component,
  ElementRef,
  NgZone,
  OnDestroy,
  OnInit,
  ViewChild,
  inject,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import {
  CandlestickSeries,
  CrosshairMode,
  HistogramSeries,
  createChart,
  type IChartApi,
  type ISeriesApi,
  type Time,
} from 'lightweight-charts';
import { firstValueFrom } from 'rxjs';

import { FrontendTelemetryService } from './telemetry';

interface CatalogTable {
  name: string;
  qualified_name: string;
}

interface CatalogLayer {
  name: string;
  tables: CatalogTable[];
}

interface CatalogResponse {
  layers: CatalogLayer[];
}

interface QueryColumn {
  name: string;
  type: string;
}

type QueryValue = string | number | boolean | null;

interface QueryResponse {
  columns: QueryColumn[];
  rows: QueryValue[][];
  row_count: number;
  truncated: boolean;
  max_rows: number;
}

interface Candle {
  ticker: string;
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume_total: number;
}

interface GoldTickersResponse {
  tickers: string[];
}

interface GoldCandlesResponse {
  ticker: string;
  candles: Candle[];
}

type AppView = 'candles' | 'explorer';

interface TimeframeOption {
  id: string;
  available: boolean;
}

const INITIAL_YEAR = 2025;
const OLDEST_YEAR = 2022;
const HISTORY_THRESHOLD = 12;

@Component({
  selector: 'app-root',
  imports: [FormsModule],
  templateUrl: './app.html',
  styleUrl: './app.css',
})
export class AppComponent implements OnInit, AfterViewInit, OnDestroy {
  @ViewChild('chartContainer')
  private chartContainer?: ElementRef<HTMLDivElement>;

  private readonly http = inject(HttpClient);
  private readonly telemetry = inject(FrontendTelemetryService);
  private readonly zone = inject(NgZone);
  private chart?: IChartApi;
  private candleSeries?: ISeriesApi<'Candlestick'>;
  private volumeSeries?: ISeriesApi<'Histogram'>;
  private resizeObserver?: ResizeObserver;
  private readonly candlesByTime = new Map<string, Candle>();
  private readonly requestedYears = new Set<number>();
  private requestGeneration = 0;
  private historyScrollEnabled = false;
  private historyLoadInProgress = false;
  private nextHistoryYear = INITIAL_YEAR - 1;

  layers: CatalogLayer[] = [];
  sql = '';
  result: QueryResponse | null = null;
  catalogError = '';
  errorMessage = '';
  isLoadingCatalog = true;
  isRunningQuery = false;
  hasRunQuery = false;
  activeView: AppView = 'candles';
  availableTickers: string[] = [];
  tickerSearch = '';
  selectedTicker = '';
  candles: Candle[] = [];
  hoveredCandle: Candle | null = null;
  loadedYears: number[] = [];
  isLoadingTickers = true;
  isLoadingCandles = false;
  candlesError = '';
  reachedOldestData = false;
  readonly timeframes: TimeframeOption[] = [
    { id: '5min', available: false },
    { id: '1D', available: true },
    { id: '1M', available: false },
  ];
  selectedTimeframe = '1D';

  ngOnInit(): void {
    this.telemetry.emit('aplicacao_iniciada');
    this.loadCatalog();
  }

  ngAfterViewInit(): void {
    this.createCandlesChart();
    this.loadAvailableTickers();
  }

  ngOnDestroy(): void {
    this.resizeObserver?.disconnect();
    this.chart?.remove();
  }

  get filteredTickers(): string[] {
    const filter = this.tickerSearch.trim().toUpperCase();
    return filter
      ? this.availableTickers.filter((ticker) => ticker.includes(filter))
      : this.availableTickers;
  }

  loadCatalog(): void {
    const startedAt = performance.now();
    this.isLoadingCatalog = true;
    this.http.get<CatalogResponse>('/api/catalog').subscribe({
      next: (response) => {
        this.layers = response.layers;
        this.catalogError = '';
        this.isLoadingCatalog = false;
        this.telemetry.emit('catalogo_carregado', {
          duration_ms: Math.round(performance.now() - startedAt),
          catalog_tables: response.layers.reduce(
            (total, layer) => total + layer.tables.length,
            0,
          ),
        });
        const preferredLayer = response.layers.find(
          (layer) => layer.name === 'silver' && layer.tables.length > 0,
        );
        const firstTable =
          preferredLayer?.tables[0] ??
          response.layers.find((layer) => layer.tables.length > 0)?.tables[0];
        if (firstTable && !this.sql.trim()) {
          this.selectTable(firstTable);
        }
      },
      error: (error: HttpErrorResponse) => {
        this.isLoadingCatalog = false;
        this.catalogError = this.errorDetail(error);
        this.telemetry.emit('falha_ao_carregar_catalogo', {
          duration_ms: Math.round(performance.now() - startedAt),
        });
      },
    });
  }

  selectTable(table: CatalogTable): void {
    this.sql = `SELECT *\nFROM ${table.qualified_name}\nLIMIT 100;`;
    this.errorMessage = '';
    this.activeView = 'explorer';
  }

  setActiveView(view: AppView): void {
    this.activeView = view;
    if (view === 'candles') {
      requestAnimationFrame(() => this.resizeChart());
    }
  }

  setTimeframe(timeframe: TimeframeOption): void {
    if (!timeframe.available || timeframe.id === this.selectedTimeframe) {
      return;
    }
    this.selectedTimeframe = timeframe.id;
  }

  onTickerChange(ticker: string): void {
    if (!ticker || ticker === this.selectedTicker) {
      return;
    }
    this.selectedTicker = ticker;
    this.requestGeneration += 1;
    this.candlesByTime.clear();
    this.candles = [];
    this.hoveredCandle = null;
    this.loadedYears = [];
    this.requestedYears.clear();
    this.nextHistoryYear = INITIAL_YEAR - 1;
    this.historyLoadInProgress = false;
    this.historyScrollEnabled = false;
    this.reachedOldestData = false;
    this.candlesError = '';
    this.candleSeries?.setData([]);
    this.volumeSeries?.setData([]);
    this.chart?.timeScale().fitContent();
    void this.loadCandlesYear(INITIAL_YEAR, true);
  }

  runQuery(): void {
    const startedAt = performance.now();
    this.isRunningQuery = true;
    this.hasRunQuery = true;
    this.result = null;
    this.errorMessage = '';
    this.telemetry.emit('consulta_iniciada');
    this.http.post<QueryResponse>('/api/query', { sql: this.sql }).subscribe({
      next: (response) => {
        this.result = response;
        this.isRunningQuery = false;
        this.telemetry.emit('consulta_concluida', {
          duration_ms: Math.round(performance.now() - startedAt),
          result_rows: response.row_count,
        });
      },
      error: (error: HttpErrorResponse) => {
        this.errorMessage = this.errorDetail(error);
        this.isRunningQuery = false;
        this.telemetry.emit('falha_na_consulta', {
          duration_ms: Math.round(performance.now() - startedAt),
        });
      },
    });
  }

  clearQuery(): void {
    this.sql = '';
    this.result = null;
    this.errorMessage = '';
    this.hasRunQuery = false;
  }

  formatPrice(value: number): string {
    return value.toLocaleString('pt-BR', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }

  private createCandlesChart(): void {
    const container = this.chartContainer?.nativeElement;
    if (!container) {
      return;
    }

    this.chart = createChart(container, {
      width: container.clientWidth || 800,
      height: container.clientHeight || 480,
      layout: {
        background: { color: '#ffffff' },
        textColor: '#52637a',
        fontFamily: 'Arial, Helvetica, sans-serif',
      },
      grid: {
        vertLines: { color: '#f0f2f5' },
        horzLines: { color: '#f0f2f5' },
      },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { borderColor: '#e1e6ee' },
      timeScale: {
        borderColor: '#e1e6ee',
        rightOffset: 4,
        fixLeftEdge: false,
        timeVisible: false,
      },
    });
    this.candleSeries = this.chart.addSeries(CandlestickSeries, {
      upColor: '#16856b',
      downColor: '#d94f5c',
      borderUpColor: '#16856b',
      borderDownColor: '#d94f5c',
      wickUpColor: '#16856b',
      wickDownColor: '#d94f5c',
    });
    this.volumeSeries = this.chart.addSeries(HistogramSeries, {
      priceScaleId: 'volume',
      priceFormat: { type: 'volume' },
      lastValueVisible: false,
      priceLineVisible: false,
    });
    this.chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.82, bottom: 0 },
      visible: false,
    });
    this.chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
      if (
        range &&
        this.historyScrollEnabled &&
        range.from <= HISTORY_THRESHOLD
      ) {
        void this.loadNextHistoryYear();
      }
    });
    this.chart.subscribeCrosshairMove((event) => {
      const item = event.seriesData.get(this.candleSeries!);
      if (!item || !('open' in item)) {
        this.zone.run(() => (this.hoveredCandle = null));
        return;
      }
      const key = this.timeKey(item.time);
      this.zone.run(() => {
        this.hoveredCandle = key ? this.candlesByTime.get(key) ?? null : null;
      });
    });
    container.addEventListener('pointerdown', this.enableHistoryLoading, {
      passive: true,
    });
    container.addEventListener('wheel', this.enableHistoryLoading, {
      passive: true,
    });
    container.addEventListener('touchstart', this.enableHistoryLoading, {
      passive: true,
    });
    this.resizeObserver = new ResizeObserver(() => this.resizeChart());
    this.resizeObserver.observe(container);
  }

  private readonly enableHistoryLoading = (): void => {
    this.historyScrollEnabled = true;
  };

  private resizeChart(): void {
    const container = this.chartContainer?.nativeElement;
    if (container && this.chart && container.clientWidth > 0) {
      this.chart.applyOptions({
        width: container.clientWidth,
        height: container.clientHeight || 480,
      });
    }
  }

  private loadAvailableTickers(): void {
    this.isLoadingTickers = true;
    this.http.get<GoldTickersResponse>('/api/candles/tickers').subscribe({
      next: (response) => {
        this.availableTickers = response.tickers;
        this.isLoadingTickers = false;
        if (this.availableTickers.length === 0) {
          this.candlesError = 'Nenhum ticker Gold com candles disponível.';
          return;
        }
        const initialTicker = this.availableTickers.includes('VALE3')
          ? 'VALE3'
          : this.availableTickers[0];
        this.onTickerChange(initialTicker);
      },
      error: (error: HttpErrorResponse) => {
        this.isLoadingTickers = false;
        this.candlesError = this.errorDetail(error);
      },
    });
  }

  private async loadCandlesYear(year: number, initial = false): Promise<void> {
    if (!this.selectedTicker || this.requestedYears.has(year)) {
      return;
    }
    this.requestedYears.add(year);
    this.isLoadingCandles = true;
    this.candlesError = '';
    const generation = this.requestGeneration;
    try {
      const response = await firstValueFrom(
        this.http.get<GoldCandlesResponse>(
          `/api/candles/${encodeURIComponent(this.selectedTicker)}`,
          { params: { year } },
        ),
      );
      if (generation !== this.requestGeneration) {
        return;
      }
      const oldTimes = new Set(this.candlesByTime.keys());
      for (const candle of response.candles) {
        this.candlesByTime.set(candle.time, candle);
      }
      this.candles = [...this.candlesByTime.values()].sort((left, right) =>
        left.time.localeCompare(right.time),
      );
      if (!this.loadedYears.includes(year)) {
        this.loadedYears = [...this.loadedYears, year].sort((a, b) => b - a);
      }
      this.updateChartData();

      if (initial) {
        this.chart?.timeScale().fitContent();
      } else {
        const addedOlderCandles = this.candles.filter(
          (candle) => !oldTimes.has(candle.time),
        ).filter((candle) => {
          const currentOldest = [...oldTimes].sort()[0];
          return !currentOldest || candle.time < currentOldest;
        }).length;
        const range = this.chart?.timeScale().getVisibleLogicalRange();
        if (range && addedOlderCandles > 0) {
          this.chart?.timeScale().setVisibleLogicalRange({
            from: range.from + addedOlderCandles,
            to: range.to + addedOlderCandles,
          });
        }
      }
      this.nextHistoryYear = Math.min(this.nextHistoryYear, year - 1);
      this.reachedOldestData = this.nextHistoryYear < OLDEST_YEAR;
      if (
        !initial &&
        response.candles.length === 0 &&
        this.nextHistoryYear >= OLDEST_YEAR &&
        this.historyScrollEnabled
      ) {
        await this.loadNextHistoryYear();
      }
    } catch (error) {
      if (generation === this.requestGeneration) {
        this.requestedYears.delete(year);
        this.candlesError = this.errorDetail(error as HttpErrorResponse);
      }
    } finally {
      if (generation === this.requestGeneration) {
        this.isLoadingCandles = false;
        this.historyLoadInProgress = false;
      }
    }
  }

  private async loadNextHistoryYear(): Promise<void> {
    if (
      this.historyLoadInProgress ||
      this.isLoadingCandles ||
      this.nextHistoryYear < OLDEST_YEAR
    ) {
      if (this.nextHistoryYear < OLDEST_YEAR) {
        this.reachedOldestData = true;
      }
      return;
    }
    this.historyLoadInProgress = true;
    const year = this.nextHistoryYear;
    await this.loadCandlesYear(year);
  }

  private updateChartData(): void {
    this.candleSeries?.setData(
      this.candles.map((candle) => ({
        time: candle.time as Time,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
      })),
    );
    this.volumeSeries?.setData(
      this.candles.map((candle) => ({
        time: candle.time as Time,
        value: candle.volume_total,
        color: candle.close >= candle.open ? '#16856b55' : '#d94f5c55',
      })),
    );
  }

  private timeKey(time: Time): string | null {
    if (typeof time === 'string') {
      return time;
    }
    if (typeof time === 'object') {
      return `${time.year}-${String(time.month).padStart(2, '0')}-${String(
        time.day,
      ).padStart(2, '0')}`;
    }
    return null;
  }

  private errorDetail(error: unknown): string {
    if (error instanceof HttpErrorResponse) {
      const detail: unknown = error.error?.detail;
      if (typeof detail === 'string') {
        return detail;
      }
      return error.message || 'Não foi possível concluir a operação.';
    }
    return error instanceof Error
      ? error.message
      : 'Não foi possível concluir a operação.';
  }
}
