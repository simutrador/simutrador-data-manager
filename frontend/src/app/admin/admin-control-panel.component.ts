import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { TradingDataRetrievalComponent } from './components/trading-data-retrieval/trading-data-retrieval.component';
import { NightlyUpdateComponent } from './components/nightly-update/nightly-update.component';
import { DataCompletenessAnalysisComponent } from './components/data-completeness-analysis/data-completeness-analysis.component';

@Component({
  selector: 'app-admin-control-panel',
  standalone: true,
  imports: [
    CommonModule,
    TradingDataRetrievalComponent,
    NightlyUpdateComponent,
    DataCompletenessAnalysisComponent,
  ],
  templateUrl: './admin-control-panel.component.html',
  styleUrls: ['./admin-control-panel.component.css'],
})
export class AdminControlPanelComponent implements OnInit {
  constructor() {}

  ngOnInit() {
    // Component initialization
  }
}
