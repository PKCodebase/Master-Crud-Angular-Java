import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, tap } from 'rxjs';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class AuthService {
  private baseUrl = environment.authUrl;
  //private apiUrl = 'http://localhost:8080/auth'; // backend login API

  constructor(private http: HttpClient) {}

  login(username: string, password: string): Observable<any> {
    this.logout();
    console.log('baseUrl--->'+this.baseUrl);
    console.log('username--->'+username);
    console.log('password--->'+password);
    return this.http.post<any>(`${this.baseUrl}`, { username, password })
    .pipe(
      tap(res => {
        if (res.token) {
          // ✅ Always replace old token with fresh one
          this.saveToken(res.token);
        }
      })
    );
  }

  saveToken(token: string) {
    console.log('inside saveToken--->'+token);
    localStorage.setItem('token', token);
  }

  getToken(): string | null {
    return localStorage.getItem('token');
  }

  isLoggedIn(): boolean {
    return !!this.getToken(); 
  }

  logout() {
    localStorage.removeItem('token');
  }
}
