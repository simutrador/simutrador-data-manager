import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { AdminService } from '../../admin.service';
import { DataCompletenessResponse } from '../../../api/models/data-completeness-response';

interface DataCompletenessRequest {
  symbols: string[];
  start_date: string;
  end_date: string;
  include_details: boolean;
  auto_fill_gaps?: boolean;
  max_gap_fill_attempts?: number;
}

@Component({
  selector: 'app-data-completeness-analysis',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './data-completeness-analysis.component.html',
  styleUrls: ['./data-completeness-analysis.component.css'],
})
export class DataCompletenessAnalysisComponent implements OnInit {
  // Make Object available in template
  Object = Object;

  // Data completeness analysis
  completenessRequest: DataCompletenessRequest = {
    symbols: [],
    start_date: '',
    end_date: '',
    include_details: false,
    auto_fill_gaps: false,
    max_gap_fill_attempts: 50,
  };

  completenessResult: DataCompletenessResponse | null = null;
  isLoadingCompleteness = false;

  constructor(private readonly adminService: AdminService) {}

  ngOnInit() {
    // Set default dates (last 30 days)
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - 30);

    this.completenessRequest.start_date = startDate.toISOString().split('T')[0];
    this.completenessRequest.end_date = endDate.toISOString().split('T')[0];
  }

  onCompletenessSymbolsChange(event: Event) {
    const target = event.target as HTMLInputElement;
    const symbolsText = target.value.trim();

    if (symbolsText) {
      this.completenessRequest.symbols = symbolsText
        .split(',')
        .map((s) => s.trim().toUpperCase())
        .filter((s) => s.length > 0);
    } else {
      this.completenessRequest.symbols = [];
    }
  }

  analyzeDataCompleteness() {
    if (this.completenessRequest.symbols.length === 0) {
      alert('Please enter at least one symbol');
      return;
    }

    if (
      !this.completenessRequest.start_date ||
      !this.completenessRequest.end_date
    ) {
      alert('Please select both start and end dates');
      return;
    }

    this.isLoadingCompleteness = true;
    this.completenessResult = null;

    this.adminService
      .analyzeDataCompleteness(
        this.completenessRequest.symbols,
        this.completenessRequest.start_date,
        this.completenessRequest.end_date,
        this.completenessRequest.include_details
      )
      .subscribe({
        next: (response) => {
          this.completenessResult = response;
          this.isLoadingCompleteness = false;
          console.log('Data completeness analysis completed:', response);
        },
        error: (error) => {
          this.isLoadingCompleteness = false;
          console.error('Data completeness analysis failed:', error);
          alert(`Analysis failed: ${error.error?.detail || error.message}`);
        },
      });
  }

  hasGapFillingResults(): boolean {
    if (!this.completenessResult) return false;

    return Object.values(this.completenessResult.symbol_completeness).some(
      (symbolData: any) => symbolData.gap_fill_attempted
    );
  }

  getTotalGapsFound(): number {
    if (!this.completenessResult) return 0;

    return Object.values(this.completenessResult.symbol_completeness).reduce(
      (total: number, symbolData: any) =>
        total + (symbolData.total_gaps_found || 0),
      0
    );
  }

  getGapsFilledSuccessfully(): number {
    if (!this.completenessResult) return 0;

    return Object.values(this.completenessResult.symbol_completeness).reduce(
      (total: number, symbolData: any) =>
        total + (symbolData.gaps_filled_successfully || 0),
      0
    );
  }

  getGapsVendorUnavailable(): number {
    if (!this.completenessResult) return 0;

    return Object.values(this.completenessResult.symbol_completeness).reduce(
      (total: number, symbolData: any) =>
        total + (symbolData.gaps_vendor_unavailable || 0),
      0
    );
  }

  getCandlesRecovered(): number {
    if (!this.completenessResult) return 0;

    return Object.values(this.completenessResult.symbol_completeness).reduce(
      (total: number, symbolData: any) =>
        total + (symbolData.candles_recovered || 0),
      0
    );
  }

  formatDateTime(dateTimeString: string): string {
    if (!dateTimeString) return '';
    try {
      const date = new Date(dateTimeString);
      return date.toLocaleString('en-US', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
      });
    } catch (error) {
      return dateTimeString; // Return original string if parsing fails
    }
  }
}
