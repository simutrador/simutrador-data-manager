import { Injectable } from '@angular/core';
import { HttpInterceptor, HttpRequest, HttpHandler, HttpEvent, HttpErrorResponse } from '@angular/common/http';
import { Observable, throwError } from 'rxjs';
import { catchError } from 'rxjs/operators';

@Injectable()
export class ErrorHandlerInterceptor implements HttpInterceptor {

  intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    return next.handle(req).pipe(
      catchError((error: HttpErrorResponse) => {
        let errorMessage = 'An unknown error occurred';
        
        if (error.error instanceof ErrorEvent) {
          // Client-side error
          errorMessage = `Client Error: ${error.error.message}`;
        } else {
          // Server-side error
          switch (error.status) {
            case 400:
              errorMessage = `Bad Request: ${error.error?.detail || 'Invalid request'}`;
              break;
            case 401:
              errorMessage = 'Unauthorized: Please check your credentials';
              break;
            case 403:
              errorMessage = 'Forbidden: You do not have permission to access this resource';
              break;
            case 404:
              errorMessage = `Not Found: ${error.error?.detail || 'Resource not found'}`;
              break;
            case 500:
              errorMessage = `Server Error: ${error.error?.detail || 'Internal server error'}`;
              break;
            case 502:
              errorMessage = 'Bad Gateway: Server is temporarily unavailable';
              break;
            case 503:
              errorMessage = 'Service Unavailable: Server is temporarily unavailable';
              break;
            default:
              errorMessage = `HTTP Error ${error.status}: ${error.error?.detail || error.message}`;
          }
        }

        // Log error to console for debugging
        console.error('HTTP Error:', {
          status: error.status,
          message: errorMessage,
          url: error.url,
          error: error.error
        });

        // You could also send errors to a logging service here
        // this.loggingService.logError(error);

        return throwError(() => new Error(errorMessage));
      })
    );
  }
}
