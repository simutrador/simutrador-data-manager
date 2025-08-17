import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { AdminService } from '../../admin.service';
import { NightlyUpdateRequest, ProgressInfo } from '../../../api/models';

interface NightlyUpdateStatus {
  isRunning: boolean;
  lastRun: string | null;
  nextRun: string | null;
  status: string;
}

interface ActiveUpdate {
  request_id: string;
  status: string;
  started_at: string;
  symbols_count: number;
  duration_seconds: number;
}

@Component({
  selector: 'app-nightly-update',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './nightly-update.component.html',
  styleUrls: ['./nightly-update.component.css'],
})
export class NightlyUpdateComponent implements OnInit, OnDestroy {
  // Make Object available in template
  Object = Object;

  // Utility method to format datetime strings for display
  formatDateTime(isoString: string): string {
    try {
      const date = new Date(isoString);
      return date.toLocaleString('en-US', {
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        timeZoneName: 'short',
      });
    } catch (e) {
      return isoString; // Return original string if parsing fails
    }
  }
  nightlyRequest: NightlyUpdateRequest = {
    symbols: null,
    force_validation: true,
    max_concurrent: 5,
    enable_resampling: true,
    start_date: null,
    end_date: null,
  };

  updateStatus: NightlyUpdateStatus = {
    isRunning: false,
    lastRun: null,
    nextRun: null,
    status: 'idle',
  };

  // Form fields
  symbolsInput = '';
  useDefaultSymbols = true;
  useCustomDateRange = false;

  // Status tracking
  currentRequestId: string | null = null;
  progressInfo: ProgressInfo | null = null;
  activeUpdates: ActiveUpdate[] = [];
  private progressInterval: any = null;

  // UI state
  isLoading = false;
  isLoadingStatus = false;
  showProgressDetails = false;

  constructor(private readonly adminService: AdminService) {}

  ngOnInit() {
    this.loadActiveUpdates();
  }

  ngOnDestroy() {
    this.clearProgressTracking();
  }

  private clearProgressTracking() {
    if (this.progressInterval) {
      clearInterval(this.progressInterval);
      this.progressInterval = null;
    }
  }

  loadActiveUpdates() {
    this.isLoadingStatus = true;
    this.adminService.getActiveNightlyUpdates().subscribe({
      next: (updates) => {
        console.log('Active updates loaded:', updates); // Debug log
        this.activeUpdates = updates;
        this.isLoadingStatus = false;

        // If there's an active update, start tracking it
        if (updates.length > 0 && !this.currentRequestId) {
          console.log('Found active update, starting tracking:', updates[0]); // Debug log
          this.currentRequestId = updates[0].request_id;
          this.updateStatus.isRunning = true;
          this.updateStatus.status = updates[0].status;
          this.startProgressTracking();
        }
      },
      error: (error) => {
        console.error('Failed to load active updates:', error);
        this.isLoadingStatus = false;
      },
    });
  }

  onSymbolsToggle() {
    if (this.useDefaultSymbols) {
      this.nightlyRequest.symbols = null;
      this.symbolsInput = '';
    } else {
      this.nightlyRequest.symbols = [];
    }
  }

  onSymbolsInputChange() {
    if (!this.useDefaultSymbols && this.symbolsInput.trim()) {
      this.nightlyRequest.symbols = this.symbolsInput
        .split(',')
        .map((s) => s.trim().toUpperCase())
        .filter((s) => s.length > 0);
    }
  }

  onDateRangeToggle() {
    if (!this.useCustomDateRange) {
      // Reset to automatic date range
      this.nightlyRequest.start_date = null;
      this.nightlyRequest.end_date = null;
    } else {
      // Set default values for custom date range
      const today = new Date();
      const thirtyDaysAgo = new Date(today);
      thirtyDaysAgo.setDate(today.getDate() - 30);

      this.nightlyRequest.start_date = thirtyDaysAgo
        .toISOString()
        .split('T')[0];
      this.nightlyRequest.end_date = today.toISOString().split('T')[0];
    }
  }

  startNightlyUpdate() {
    // Validate input
    if (
      !this.useDefaultSymbols &&
      (!this.nightlyRequest.symbols || this.nightlyRequest.symbols.length === 0)
    ) {
      alert('Please enter at least one symbol or use default symbols');
      return;
    }

    this.isLoading = true;

    this.adminService.startNightlyUpdate(this.nightlyRequest).subscribe({
      next: (response) => {
        console.log('Nightly update started:', response); // Debug log
        this.isLoading = false;
        this.currentRequestId = response.request_id;
        this.updateStatus.isRunning = true;
        this.updateStatus.status = 'running';
        console.log('Starting progress tracking for:', this.currentRequestId); // Debug log
        this.startProgressTracking();
        alert(
          `Nightly update started successfully! Request ID: ${response.request_id}`
        );
      },
      error: (error) => {
        this.isLoading = false;
        console.error('Failed to start nightly update:', error);
        alert(
          `Failed to start nightly update: ${
            error.error?.detail || error.message || 'Unknown error'
          }`
        );
      },
    });
  }

  startProgressTracking() {
    if (!this.currentRequestId) return;

    // Clear any existing interval
    this.clearProgressTracking();

    // Poll for status every 2 seconds using the new typed response
    this.progressInterval = setInterval(() => {
      if (!this.currentRequestId) {
        this.clearProgressTracking();
        return;
      }

      // Use the new typed getNightlyUpdateStatus method
      this.adminService
        .getNightlyUpdateStatus(this.currentRequestId)
        .subscribe({
          next: (statusResponse) => {
            console.log('Status update received:', statusResponse); // Debug log

            // Now we have proper TypeScript types!
            this.progressInfo = statusResponse.progress;
            this.updateStatus.status = statusResponse.status;
            this.updateStatus.isRunning = !statusResponse.is_complete;

            // Check if completed using the typed response
            if (statusResponse.is_complete) {
              this.clearProgressTracking();
              this.updateStatus.isRunning = false;
              this.updateStatus.status = 'completed';
              this.loadActiveUpdates();

              // Log completion details if available
              if (statusResponse.summary) {
                console.log(
                  'Update completed with summary:',
                  statusResponse.summary
                );
              }
            }
          },
          error: (error) => {
            console.error('Failed to get status:', error);
            this.clearProgressTracking();
          },
        });
    }, 2000);
  }
}
