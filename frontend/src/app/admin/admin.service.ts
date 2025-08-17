import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

// Import auto-generated OpenAPI models and services
import {
  PriceDataSeries,
  NightlyUpdateRequest,
  NightlyUpdateResponse,
  DataCompletenessRequest,
  DataCompletenessResponse,
  UpdateStatusResponse,
  UpdateProgressDetailsResponse,
  ActiveUpdateSummary,
} from '../api/models';

import {
  TradingDataService,
  NightlyUpdateService,
  DataAnalysisService,
} from '../api/services';

@Injectable({
  providedIn: 'root',
})
export class AdminService {
  constructor(
    private readonly tradingDataService: TradingDataService,
    private readonly nightlyUpdateService: NightlyUpdateService,
    private readonly dataAnalysisService: DataAnalysisService
  ) {}

  // Note: Manual trading data update has been removed.
  // Use nightly update methods below for data updates.

  getTradingData(
    symbol: string,
    timeframe: string = '1min',
    startDate?: string,
    endDate?: string,
    orderBy: string = 'desc',
    page: number = 1,
    pageSize: number = 1000
  ): Observable<PriceDataSeries> {
    return this.tradingDataService.getTradingDataTradingDataDataSymbolGet({
      symbol,
      timeframe,
      start_date: startDate,
      end_date: endDate,
      order_by: orderBy,
      page,
      page_size: pageSize,
    });
  }

  getStoredSymbols(timeframe: string = '1min'): Observable<string[]> {
    return this.tradingDataService.listStoredSymbolsTradingDataSymbolsGet({
      timeframe,
    });
  }

  // Nightly Update API
  startNightlyUpdate(request: NightlyUpdateRequest): Observable<any> {
    return this.nightlyUpdateService.startNightlyUpdateNightlyUpdateStartPost({
      body: request,
    });
  }

  getNightlyUpdateStatus(requestId: string): Observable<UpdateStatusResponse> {
    return this.nightlyUpdateService.getUpdateStatusNightlyUpdateStatusRequestIdGet(
      {
        request_id: requestId,
      }
    );
  }

  getNightlyUpdateProgress(
    requestId: string
  ): Observable<UpdateProgressDetailsResponse> {
    return this.nightlyUpdateService.getUpdateProgressDetailsNightlyUpdateStatusRequestIdProgressGet(
      {
        request_id: requestId,
      }
    );
  }

  getNightlyUpdateDetails(
    requestId: string
  ): Observable<NightlyUpdateResponse> {
    return this.nightlyUpdateService.getUpdateDetailsNightlyUpdateStatusRequestIdDetailsGet(
      {
        request_id: requestId,
      }
    );
  }

  getActiveNightlyUpdates(): Observable<ActiveUpdateSummary[]> {
    return this.nightlyUpdateService.listActiveUpdatesNightlyUpdateActiveGet();
  }

  analyzeDataCompleteness(
    symbols: string[],
    startDate: string,
    endDate: string,
    includeDetails: boolean = false
  ): Observable<DataCompletenessResponse> {
    const request: DataCompletenessRequest = {
      symbols,
      start_date: startDate,
      end_date: endDate,
      include_details: includeDetails,
    };
    return this.dataAnalysisService.analyzeDataCompletenessDataAnalysisCompletenessPost(
      {
        body: request,
      }
    );
  }
}
