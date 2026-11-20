import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Component, OnInit, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';

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

@Component({
  selector: 'app-root',
  imports: [FormsModule],
  templateUrl: './app.html',
  styleUrl: './app.css',
})
export class AppComponent implements OnInit {
  private readonly http = inject(HttpClient);

  layers: CatalogLayer[] = [];
  sql = '';
  result: QueryResponse | null = null;
  catalogError = '';
  errorMessage = '';
  isLoadingCatalog = true;
  isRunningQuery = false;
  hasRunQuery = false;

  ngOnInit(): void {
    this.loadCatalog();
  }

  loadCatalog(): void {
    this.isLoadingCatalog = true;
    this.http.get<CatalogResponse>('/api/catalog').subscribe({
      next: (response) => {
        this.layers = response.layers;
        this.catalogError = '';
        this.isLoadingCatalog = false;
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
      },
    });
  }

  selectTable(table: CatalogTable): void {
    this.sql = `SELECT *\nFROM ${table.qualified_name}\nLIMIT 100;`;
    this.errorMessage = '';
  }

  runQuery(): void {
    this.isRunningQuery = true;
    this.hasRunQuery = true;
    this.result = null;
    this.errorMessage = '';
    this.http.post<QueryResponse>('/api/query', { sql: this.sql }).subscribe({
      next: (response) => {
        this.result = response;
        this.isRunningQuery = false;
      },
      error: (error: HttpErrorResponse) => {
        this.errorMessage = this.errorDetail(error);
        this.isRunningQuery = false;
      },
    });
  }

  clearQuery(): void {
    this.sql = '';
    this.result = null;
    this.errorMessage = '';
    this.hasRunQuery = false;
  }

  private errorDetail(error: HttpErrorResponse): string {
    const detail: unknown = error.error?.detail;
    if (typeof detail === 'string') {
      return detail;
    }
    return error.message || 'Não foi possível concluir a operação.';
  }
}
