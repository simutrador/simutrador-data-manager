import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { AdminService } from '../../admin.service';
import { PaginationInfo, PriceCandle } from '../../../api/models';

interface DataRequest {
  symbols: string[];
  timeframe: string;
  startDate: string;
  endDate: string;
  page: number;
  pageSize: number;
}

@Component({
  selector: 'app-trading-data-retrieval',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './trading-data-retrieval.component.html',
  styleUrls: ['./trading-data-retrieval.component.css'],
})
export class TradingDataRetrievalComponent implements OnInit {
  symbolsInput = '';

  dataRequest: DataRequest = {
    symbols: [],
    timeframe: '1min',
    startDate: '',
    endDate: '',
    page: 1,
    pageSize: 1000,
  };

  retrievedData: PriceCandle[] = [];
  paginationInfo: PaginationInfo | null = null;
  isLoadingData = false;

  availableTimeframes = [
    '1min',
    '5min',
    '15min',
    '30min',
    '1h',
    '2h',
    '4h',
    'daily',
  ];

  constructor(private readonly adminService: AdminService) {}

  ngOnInit() {
    // Component initialization
  }

  onSymbolsInputChange() {
    const symbolsText = this.symbolsInput.trim();

    if (symbolsText) {
      this.dataRequest.symbols = symbolsText
        .split(',')
        .map((s) => s.trim().toUpperCase())
        .filter((s) => s.length > 0);
    } else {
      this.dataRequest.symbols = [];
    }
  }

  fetchData() {
    if (this.dataRequest.symbols.length === 0) {
      alert('Please enter at least one symbol');
      return;
    }

    this.isLoadingData = true;
    this.retrievedData = [];

    // For now, fetch data for the first symbol only
    // TODO: Update backend to support multiple symbols or make multiple calls
    const symbol = this.dataRequest.symbols[0];

    this.adminService
      .getTradingData(
        symbol,
        this.dataRequest.timeframe,
        this.dataRequest.startDate || undefined,
        this.dataRequest.endDate || undefined,
        'desc', // orderBy
        this.dataRequest.page,
        this.dataRequest.pageSize
      )
      .subscribe({
        next: (response) => {
          this.retrievedData = response.candles;
          this.paginationInfo = response.pagination || null;
          this.isLoadingData = false;

          console.log(
            `Fetched ${response.candles.length} candles for ${response.symbol} (${response.timeframe})`
          );

          if (this.paginationInfo) {
            console.log(
              `Page ${this.paginationInfo.page} of ${this.paginationInfo.total_pages} (${this.paginationInfo.total_items} total items)`
            );
          }

          if (response.candles.length === 0) {
            alert(
              `No data found for ${this.dataRequest.symbols} with the specified parameters`
            );
          }
        },
        error: (error) => {
          this.isLoadingData = false;
          this.retrievedData = [];

          console.error('Data fetch failed:', error);

          if (error.status === 404) {
            alert(
              `No data found for ${this.dataRequest.symbols} ${this.dataRequest.timeframe}`
            );
          } else {
            alert(
              `Failed to fetch data: ${
                error.error?.detail || error.message || 'Unknown error'
              }`
            );
          }
        },
      });
  }

  // Pagination methods
  goToPage(page: number) {
    if (
      page >= 1 &&
      this.paginationInfo &&
      page <= this.paginationInfo.total_pages
    ) {
      this.dataRequest.page = page;
      this.fetchData();
    }
  }

  goToFirstPage() {
    this.goToPage(1);
  }

  goToPreviousPage() {
    if (this.paginationInfo && this.paginationInfo.has_previous) {
      this.goToPage(this.dataRequest.page - 1);
    }
  }

  goToNextPage() {
    if (this.paginationInfo && this.paginationInfo.has_next) {
      this.goToPage(this.dataRequest.page + 1);
    }
  }

  goToLastPage() {
    if (this.paginationInfo) {
      this.goToPage(this.paginationInfo.total_pages);
    }
  }

  onPageSizeChange() {
    this.dataRequest.page = 1; // Reset to first page when page size changes
    this.fetchData();
  }
}
