import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';

import { AdminControlPanelComponent } from './admin-control-panel.component';
import { AdminService } from './admin.service';
import { TradingDataRetrievalComponent } from './components/trading-data-retrieval/trading-data-retrieval.component';
import { NightlyUpdateComponent } from './components/nightly-update/nightly-update.component';

describe('AdminControlPanelComponent', () => {
  let component: AdminControlPanelComponent;
  let fixture: ComponentFixture<AdminControlPanelComponent>;
  let mockAdminService: jasmine.SpyObj<AdminService>;

  beforeEach(async () => {
    mockAdminService = jasmine.createSpyObj('AdminService', [
      'getStoredSymbols',
      'startNightlyUpdate',
      'getActiveNightlyUpdates',
      'getNightlyUpdateProgress',
    ]);

    // Setup default return values for the mocked methods
    mockAdminService.getStoredSymbols.and.returnValue(
      of(['AAPL', 'MSFT', 'GOOGL'])
    );
    mockAdminService.getActiveNightlyUpdates.and.returnValue(of([]));
    mockAdminService.startNightlyUpdate.and.returnValue(
      of({ request_id: 'test-123', status: 'started', message: 'Test started' })
    );
    mockAdminService.getNightlyUpdateProgress.and.returnValue(
      of({
        request_id: 'test-123',
        overall_progress: {
          total_symbols: 0,
          completed_symbols: 0,
          current_symbol: null,
          current_step: 'starting',
          progress_percentage: 0,
          estimated_time_remaining_seconds: null,
          symbols_in_progress: [],
        },
        symbol_progress: {},
      })
    );

    await TestBed.configureTestingModule({
      imports: [
        AdminControlPanelComponent,
        TradingDataRetrievalComponent,
        NightlyUpdateComponent,
      ],
      providers: [{ provide: AdminService, useValue: mockAdminService }],
    }).compileComponents();

    fixture = TestBed.createComponent(AdminControlPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize without errors', () => {
    expect(component).toBeTruthy();
  });
});
