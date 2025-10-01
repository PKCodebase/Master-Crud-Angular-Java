import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { LoginComponent } from './auth/login/login.component';
import { DynamicFormComponent } from './components/dynamic-form/dynamic-form.component';
import { AuthGuard } from './auth/auth.guard';

//const routes: Routes = [];

const routes: Routes = [
  { path: 'login', component: LoginComponent },
  { path: 'dynamic-form', component: DynamicFormComponent, canActivate: [AuthGuard] },
  { path: '', redirectTo: '/login', pathMatch: 'full' }, // default route
  { path: '**', redirectTo: '/login' } // wildcard
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
