import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class DynamicFormService {
  //private baseUrl = 'http://localhost:8080/dynamicApi';
  private baseUrl = environment.apiUrl;

  constructor(private http: HttpClient) {}

  //Added
  getSchemas(): Observable<string[]> {
    return this.http.get<string[]>(`${this.baseUrl}/schemas`);
  }

  // ✅ Get tables for schema
  getTables(schema: string): Observable<string[]> {
    return this.http.get<string[]>(`${this.baseUrl}/tables/${schema}`);
  }

  getAllTables(): Observable<{ [schema: string]: string[] }> {
    return this.http.get<{ [schema: string]: string[] }>(`${this.baseUrl}/tables`);
  }

  getColumns(schema: string, table: string): Observable<any[]> {
    return this.http.get<any[]>(`${this.baseUrl}/${schema}/${table}/columns`);
  }

  getAll(schema: string, table: string): Observable<any[]> {
    return this.http.get<any[]>(`${this.baseUrl}/${schema}/${table}`);
  }

  create(schema: string, table: string, payload: any): Observable<any> {
    console.log("payload--->"+payload);
    return this.http.post(`${this.baseUrl}/${schema}/${table}`, payload);
  }

  update(schema: string, table: string, id: any, payload: any): Observable<any> {
    return this.http.put(`${this.baseUrl}/${schema}/${table}/${id}`, payload);
  }

  delete(schema: string, table: string, id: any): Observable<any> {
    return this.http.delete(`${this.baseUrl}/${schema}/${table}/${id}`);
  }

  insertRow(schema: string, table: string, row: any): Observable<any> {
    return this.http.post(`${this.baseUrl}/${schema}/${table}`, row);
  }

  getForeignKeyValues(schema: string, table: string, column: string): Observable<any[]> {
    return this.http.get<any[]>(`${this.baseUrl}/${schema}/${table}/fk-values/${column}`);
  }

  getConstraints(schema: string, table: string): Observable<any[]> {
    return this.http.get<any[]>(`${this.baseUrl}/${schema}/${table}/constraints`);
  }
}
