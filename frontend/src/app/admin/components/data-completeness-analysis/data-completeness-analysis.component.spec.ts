import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';
import { DataCompletenessAnalysisComponent } from './data-completeness-analysis.component';
import { AdminService } from '../../admin.service';

describe('DataCompletenessAnalysisComponent', () => {
  let component: DataCompletenessAnalysisComponent;
  let fixture: ComponentFixture<DataCompletenessAnalysisComponent>;
  let mockAdminService: jasmine.SpyObj<AdminService>;

  beforeEach(async () => {
    mockAdminService = jasmine.createSpyObj('AdminService', [
      'analyzeDataCompleteness',
    ]);

    mockAdminService.analyzeDataCompleteness.and.returnValue(
      of({
        analysis_period: {
          start_date: '2025-01-01',
          end_date: '2025-01-15',
        },
        overall_statistics: {
          total_symbols: 1,
          total_trading_days: 10,
          total_valid_days: 10,
          total_expected_candles: 1000,
          total_actual_candles: 1000,
          total_missing_candles: 0,
          overall_completeness_percentage: 100,
        },
        symbol_completeness: {},
        symbols_needing_attention: [],
        recommendations: [],
      })
    );

    await TestBed.configureTestingModule({
      imports: [DataCompletenessAnalysisComponent],
      providers: [{ provide: AdminService, useValue: mockAdminService }],
    }).compileComponents();

    fixture = TestBed.createComponent(DataCompletenessAnalysisComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize with default date range', () => {
    expect(component.completenessRequest.start_date).toBeTruthy();
    expect(component.completenessRequest.end_date).toBeTruthy();
  });

  it('should parse symbols from input', () => {
    const event = {
      target: { value: 'AAPL, MSFT, GOOGL' },
    } as any;

    component.onCompletenessSymbolsChange(event);

    expect(component.completenessRequest.symbols).toEqual([
      'AAPL',
      'MSFT',
      'GOOGL',
    ]);
  });

  it('should call analyzeDataCompleteness when analyze button is clicked', () => {
    component.completenessRequest.symbols = ['AAPL'];
    component.completenessRequest.start_date = '2025-01-01';
    component.completenessRequest.end_date = '2025-01-15';

    component.analyzeDataCompleteness();

    expect(mockAdminService.analyzeDataCompleteness).toHaveBeenCalledWith(
      ['AAPL'],
      '2025-01-01',
      '2025-01-15',
      false
    );
  });
});
