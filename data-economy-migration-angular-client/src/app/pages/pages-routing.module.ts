import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { HomeComponent } from './home/home.component';
import { RequestComponent } from './request/request.component';
import { HistoryComponent } from './history/history.component';
import { ReconComponent } from './recon/recon.component';
import { HelpComponent } from './help/help.component';
import { UserComponent } from './user/user.component';
import { ConnectionComponent } from './connection/connection.component';
import { RequestPreviewComponent } from './request-preview/request-preview.component';
import { BasketComponent } from './basket/basket.component';
import { ChangePasswordComponent } from './change-password/change-password.component';

const routes: Routes = [
  { path: '', redirectTo: 'home', pathMatch: 'full' },
  { path: 'home', component: HomeComponent },
  { path: 'request', component: RequestComponent },
  { path: 'request/preview', component: RequestPreviewComponent },
  { path: 'basket', component: BasketComponent },
  { path: 'history', component: HistoryComponent },
  { path: 'recon', component: ReconComponent },
  { path: 'help', component: HelpComponent },
  { path: 'settings/users', component: UserComponent },
  { path: 'settings/connection', component: ConnectionComponent },
  { path: 'settings/change-password', component: ChangePasswordComponent }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class PagesRoutingModule { }
