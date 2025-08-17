import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { of } from 'rxjs';

import { TradingDataRetrievalComponent } from './trading-data-retrieval.component';
import { AdminService } from '../../admin.service';
import { Timeframe } from '../../../api/models';

describe('TradingDataRetrievalComponent', () => {
  let component: TradingDataRetrievalComponent;
  let fixture: ComponentFixture<TradingDataRetrievalComponent>;
  let mockAdminService: jasmine.SpyObj<AdminService>;

  beforeEach(async () => {
    mockAdminService = jasmine.createSpyObj('AdminService', ['getTradingData']);

    // Setup default return values for the mocked methods
    mockAdminService.getTradingData.and.returnValue(
      of({
        symbol: 'AAPL',
        timeframe: Timeframe.$1Min,
        candles: [],
        start_date: null,
        end_date: null,
        pagination: {
          page: 1,
          page_size: 1000,
          total_items: 5000,
          total_pages: 5,
          has_next: true,
          has_previous: false,
        },
      })
    );

    await TestBed.configureTestingModule({
      imports: [TradingDataRetrievalComponent, FormsModule],
      providers: [{ provide: AdminService, useValue: mockAdminService }],
    }).compileComponents();

    fixture = TestBed.createComponent(TradingDataRetrievalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize with default values', () => {
    expect(component.dataRequest.timeframe).toEqual('1min');
    expect(component.isLoadingData).toBeFalse();
  });

  it('should validate data request before fetching', () => {
    spyOn(window, 'alert');

    // Test with empty symbols array
    component.dataRequest.symbols = [];
    component.fetchData();
    expect(window.alert).toHaveBeenCalledWith(
      'Please enter at least one symbol'
    );
  });

  it('should call getTradingData when fetching data with valid symbols', () => {
    component.dataRequest.symbols = ['AAPL'];
    component.dataRequest.timeframe = '5min';
    component.dataRequest.startDate = '2024-01-01';
    component.dataRequest.endDate = '2024-01-31';
    component.dataRequest.page = 1;
    component.dataRequest.pageSize = 1000;

    component.fetchData();

    expect(mockAdminService.getTradingData).toHaveBeenCalledWith(
      'AAPL',
      '5min',
      '2024-01-01',
      '2024-01-31',
      'desc', // orderBy
      1, // page
      1000 // pageSize
    );
  });

  it('should parse symbols input correctly', () => {
    component.symbolsInput = 'AAPL, MSFT, GOOGL';

    component.onSymbolsInputChange();

    expect(component.dataRequest.symbols).toEqual(['AAPL', 'MSFT', 'GOOGL']);
  });

  // Pagination Tests
  describe('Pagination functionality', () => {
    beforeEach(() => {
      component.dataRequest.symbols = ['AAPL'];
      component.paginationInfo = {
        page: 2,
        page_size: 1000,
        total_items: 5000,
        total_pages: 5,
        has_next: true,
        has_previous: true,
      };
    });

    it('should go to specific page when goToPage is called with valid page number', () => {
      spyOn(component, 'fetchData');

      component.goToPage(3);

      expect(component.dataRequest.page).toBe(3);
      expect(component.fetchData).toHaveBeenCalled();
    });

    it('should not change page when goToPage is called with invalid page number', () => {
      spyOn(component, 'fetchData');
      const originalPage = component.dataRequest.page;

      component.goToPage(0); // Invalid: less than 1
      expect(component.dataRequest.page).toBe(originalPage);
      expect(component.fetchData).not.toHaveBeenCalled();

      component.goToPage(10); // Invalid: greater than total pages
      expect(component.dataRequest.page).toBe(originalPage);
      expect(component.fetchData).not.toHaveBeenCalled();
    });

    it('should go to first page when goToFirstPage is called', () => {
      spyOn(component, 'fetchData');

      component.goToFirstPage();

      expect(component.dataRequest.page).toBe(1);
      expect(component.fetchData).toHaveBeenCalled();
    });

    it('should go to previous page when goToPreviousPage is called and has_previous is true', () => {
      spyOn(component, 'fetchData');
      component.dataRequest.page = 3;

      component.goToPreviousPage();

      expect(component.dataRequest.page).toBe(2);
      expect(component.fetchData).toHaveBeenCalled();
    });

    it('should not go to previous page when has_previous is false', () => {
      spyOn(component, 'fetchData');
      component.paginationInfo!.has_previous = false;
      component.dataRequest.page = 1;

      component.goToPreviousPage();

      expect(component.dataRequest.page).toBe(1);
      expect(component.fetchData).not.toHaveBeenCalled();
    });

    it('should go to next page when goToNextPage is called and has_next is true', () => {
      spyOn(component, 'fetchData');
      component.dataRequest.page = 2;

      component.goToNextPage();

      expect(component.dataRequest.page).toBe(3);
      expect(component.fetchData).toHaveBeenCalled();
    });

    it('should not go to next page when has_next is false', () => {
      spyOn(component, 'fetchData');
      component.paginationInfo!.has_next = false;
      component.dataRequest.page = 5;

      component.goToNextPage();

      expect(component.dataRequest.page).toBe(5);
      expect(component.fetchData).not.toHaveBeenCalled();
    });

    it('should go to last page when goToLastPage is called', () => {
      spyOn(component, 'fetchData');

      component.goToLastPage();

      expect(component.dataRequest.page).toBe(5);
      expect(component.fetchData).toHaveBeenCalled();
    });

    it('should reset to first page and fetch data when page size changes', () => {
      spyOn(component, 'fetchData');
      component.dataRequest.page = 3;
      component.dataRequest.pageSize = 500;

      component.onPageSizeChange();

      expect(component.dataRequest.page).toBe(1);
      expect(component.fetchData).toHaveBeenCalled();
    });

    it('should handle pagination info from API response', () => {
      const mockResponse = {
        symbol: 'AAPL',
        timeframe: Timeframe.$1Min,
        candles: [
          /* mock candles */
        ],
        start_date: null,
        end_date: null,
        pagination: {
          page: 2,
          page_size: 1000,
          total_items: 3500,
          total_pages: 4,
          has_next: true,
          has_previous: true,
        },
      };

      mockAdminService.getTradingData.and.returnValue(of(mockResponse));
      component.dataRequest.symbols = ['AAPL'];

      component.fetchData();

      expect(component.paginationInfo).toEqual(mockResponse.pagination);
    });
  });
});
