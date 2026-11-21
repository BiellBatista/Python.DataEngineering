import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';

export type FrontendEvent =
  | 'aplicacao_iniciada'
  | 'catalogo_carregado'
  | 'falha_ao_carregar_catalogo'
  | 'consulta_iniciada'
  | 'consulta_concluida'
  | 'falha_na_consulta'
  | 'erro_na_interface';

export interface FrontendLogDetails {
  duration_ms?: number;
  catalog_tables?: number;
  result_rows?: number;
}

@Injectable({ providedIn: 'root' })
export class FrontendTelemetryService {
  private readonly http = inject(HttpClient);

  emit(event: FrontendEvent, details: FrontendLogDetails = {}): void {
    this.http.post<{ status: string }>('/api/logs', { event, ...details }).subscribe({
      error: () => {
        console.warn(
          `Não foi possível enviar o evento de observabilidade "${event}" à API.`,
        );
      },
    });
  }
}
