import { Routes } from '@angular/router';
import { AdminControlPanelComponent } from './admin/admin-control-panel.component';

export const routes: Routes = [
  { path: '', redirectTo: '/admin', pathMatch: 'full' },
  { path: 'admin', component: AdminControlPanelComponent },
  { path: '**', redirectTo: '/admin' },
];
