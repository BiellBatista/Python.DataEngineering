import { ErrorHandler, Injectable, inject } from '@angular/core';

import { FrontendTelemetryService } from './telemetry';

@Injectable()
export class FrontendErrorHandler implements ErrorHandler {
  private readonly telemetry = inject(FrontendTelemetryService);

  handleError(_error: unknown): void {
    console.error('Erro não tratado na interface do Data Explorer.');
    this.telemetry.emit('erro_na_interface');
  }
}
