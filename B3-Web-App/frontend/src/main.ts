import { ErrorHandler } from '@angular/core';
import { bootstrapApplication } from '@angular/platform-browser';
import { provideHttpClient } from '@angular/common/http';

import { AppComponent } from './app/app';
import { FrontendErrorHandler } from './app/frontend-error-handler';

bootstrapApplication(AppComponent, {
  providers: [
    provideHttpClient(),
    { provide: ErrorHandler, useClass: FrontendErrorHandler },
  ],
}).catch((_error: unknown) => {
  console.error('Não foi possível iniciar a interface do Data Process.');
});
