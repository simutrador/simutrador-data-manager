import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { NightlyUpdateComponent } from './nightly-update.component';
import { AdminService } from '../../admin.service';

describe('NightlyUpdateComponent', () => {
  let component: NightlyUpdateComponent;
  let fixture: ComponentFixture<NightlyUpdateComponent>;
  let mockAdminService: jasmine.SpyObj<AdminService>;

  beforeEach(async () => {
    mockAdminService = jasmine.createSpyObj('AdminService', [
      'startNightlyUpdate',
      'getActiveNightlyUpdates',
      'getNightlyUpdateProgress',
    ]);

    // Setup mock return values
    mockAdminService.getActiveNightlyUpdates.and.returnValue(
      jasmine.createSpyObj('Observable', ['subscribe'])
    );
    mockAdminService.startNightlyUpdate.and.returnValue(
      jasmine.createSpyObj('Observable', ['subscribe'])
    );

    await TestBed.configureTestingModule({
      imports: [NightlyUpdateComponent, FormsModule],
      providers: [{ provide: AdminService, useValue: mockAdminService }],
    }).compileComponents();

    fixture = TestBed.createComponent(NightlyUpdateComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize with default values', () => {
    expect(component.nightlyRequest.symbols).toBeNull();
    expect(component.nightlyRequest.force_validation).toBeTrue();
    expect(component.nightlyRequest.max_concurrent).toBe(5);
    expect(component.nightlyRequest.enable_resampling).toBeTrue();
    expect(component.nightlyRequest.start_date).toBeNull();
    expect(component.nightlyRequest.end_date).toBeNull();
    expect(component.updateStatus.isRunning).toBeFalse();
    expect(component.useDefaultSymbols).toBeTrue();
    expect(component.useCustomDateRange).toBeFalse();
  });

  it('should handle symbols toggle correctly', () => {
    // Test switching to custom symbols
    component.useDefaultSymbols = false;
    component.onSymbolsToggle();
    expect(component.nightlyRequest.symbols).toEqual([]);
    expect(component.symbolsInput).toBe('');

    // Test switching back to default symbols
    component.useDefaultSymbols = true;
    component.onSymbolsToggle();
    expect(component.nightlyRequest.symbols).toBeNull();
    expect(component.symbolsInput).toBe('');
  });

  it('should handle symbols input changes correctly', () => {
    component.useDefaultSymbols = false;
    component.symbolsInput = 'AAPL, msft, googl';
    component.onSymbolsInputChange();

    expect(component.nightlyRequest.symbols).toEqual(['AAPL', 'MSFT', 'GOOGL']);
  });

  it('should handle date range toggle correctly', () => {
    // Test switching to custom date range
    component.useCustomDateRange = true;
    component.onDateRangeToggle();

    expect(component.nightlyRequest.start_date).toBeTruthy();
    expect(component.nightlyRequest.end_date).toBeTruthy();

    // Test switching back to automatic date range
    component.useCustomDateRange = false;
    component.onDateRangeToggle();

    expect(component.nightlyRequest.start_date).toBeNull();
    expect(component.nightlyRequest.end_date).toBeNull();
  });

  it('should validate symbols before starting update', () => {
    spyOn(window, 'alert');

    // Test with custom symbols but no symbols entered
    component.useDefaultSymbols = false;
    component.nightlyRequest.symbols = [];
    component.startNightlyUpdate();

    expect(window.alert).toHaveBeenCalledWith(
      'Please enter at least one symbol or use default symbols'
    );
  });
});
