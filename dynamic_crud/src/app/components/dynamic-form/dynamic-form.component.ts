
import { Component, Input, OnInit, ViewChild } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatTableDataSource } from '@angular/material/table';
import { DynamicFormService } from '../../services/dynamic-form.service';
import { MatPaginator } from '@angular/material/paginator';
import { MatSort } from '@angular/material/sort';

@Component({
  selector: 'app-dynamic-form',
  templateUrl: './dynamic-form.component.html',
  styleUrls: ['./dynamic-form.component.css']
})
export class DynamicFormComponent implements OnInit {

  // Columns & rows
  columns: any[] = [];
  rows: any[] = [];
  form!: FormGroup;
  selectedRow: any = null;
  isFormReady = false;

  // Edit state
  isEdit: boolean = false;
  rowId: any = null;

  // Table datasource
  dataSource = new MatTableDataSource<any>();
  displayedColumns: string[] = [];

  schemaForm!: FormGroup;
  schemas: string[] = [];
  tablesMap: { [schema: string]: string[] } = {};
  tables: string[] = [];

  selectedSchema: string = '';
  selectedTable: string = '';

  fkOptions: { [key: string]: any[] } = {};
  filteredFkOptions: { [key: string]: any[] } = {};
  checkOptions: { [key: string]: string[] } = {};
  

  constraints: any[] = [];

  // ✅ Audit/auto fields - these will be hidden from UI
  autoFields: string[] = [
    'created_ip_addr', 'created_by', 'created_mac_addr', 'created_date','created_uri','modified_uri',
    'modified_ip_addr', 'modified_by', 'modified_date', 'modified_mac_addr'
    ,'api_service_url'
  ];

  // ✅ Filtered columns for UI display (excludes auto fields)
  get displayColumns(): any[] {
    return this.columns.filter(col => !this.autoFields.includes(col.name));
  }

  //@ViewChild it is a decorator in Angular It allows you to reference a child component,
  //  directive, or DOM element from your template. 
  // You can then access its properties and methods in your TypeScript code.
  @ViewChild(MatPaginator) paginator!: MatPaginator;  // Pagination control
  @ViewChild(MatSort) sort!: MatSort;  //Sorting Control
  router: any;


  // injecting Dependency of DynamicFormService
  // FormBuilder is used for create and manage form easily
  constructor(private formService: DynamicFormService, private fb: FormBuilder) {}


//ngOnInit() is a lifecycle hook in Angular.
//It runs automatically when your component is initialized (just after it is created).
//It’s commonly used for setup work — like creating forms, fetching data from APIs, etc.
  ngOnInit(): void {
    this.schemaForm = this.fb.group({
      schema: ['', Validators.required], //Validators is used for validation 
      table: ['', Validators.required]
    });

    // Load schemas from backend
    this.formService.getSchemas().subscribe((schemas) => {
      this.schemas = schemas;
    });
  }

  //Load table inside schema
  onSchemaChange() {                                                                                                                                                              
    const schema = this.schemaForm.value.schema;
    this.formService.getTables(schema).subscribe((tables) => {
      this.tables = tables;
    });
  }

  onSchemaSubmit() {
    this.selectedSchema = this.schemaForm.value.schema;
    this.selectedTable = this.schemaForm.value.table;

    this.loadMetadata(this.selectedSchema, this.selectedTable);
    this.loadData(this.selectedSchema, this.selectedTable);
  }

  ngAfterViewInit() {
    this.dataSource.paginator = this.paginator;
    this.dataSource.sort = this.sort;
  }

loadMetadata(schema: any, table: any): void {
  this.formService.getColumns(schema, table).subscribe(cols => {
    this.columns = cols;
    this.fkOptions = {};
    this.checkOptions = {};

    // Fetch FK values and Check constraint dropdowns
    this.displayColumns.forEach(col => {
      // 🔹 Load FK dropdowns
      if (col.isForeignKey) {
        this.formService.getForeignKeyValues(schema, table, col.name)
          .subscribe(values => this.fkOptions[col.name] = values);
      }

      // 🔹 Load CHECK constraint dropdowns
     this.formService.getCheckConstraintValues(`${schema}.${table}`, col.name)
  .subscribe(values => {
    console.log(`✅ Check values for ${col.name}:`, values);
    if (values && values.length > 0) {
      this.checkOptions[col.name] = values;
      col.hasCheckConstraint = true;
    }
  });

    });

    // Build form dynamically
    const group: { [key: string]: any } = {};
    this.columns.forEach(col => {
      const validators = col.nullable ? [] : [Validators.required];
      group[col.name] = this.fb.control(
        { value: '', disabled: col.autoIncrement || col.primaryKey || this.autoFields.includes(col.name) },
        validators
      );
    });

    this.form = this.fb.group(group);
    this.isFormReady = true;
    this.displayedColumns = [...this.displayColumns.map(c => c.name), 'actions'];
  });

  // Also fetch constraints for debugging / table view
  this.loadConstraints(schema, table);
}

  onFilterFk(colName: string, event: Event): void {
    const filterValue = (event.target as HTMLInputElement).value.toLowerCase();
    this.filteredFkOptions[colName] = this.fkOptions[colName].filter(option =>
      option.value.toLowerCase().startsWith(filterValue)
    );
  }

  // loadData(schema: any, table: any): void {
  //   this.formService.getAll(schema, table).subscribe((rows: any[]) => {
  //     this.rows = rows;
  //     this.dataSource = new MatTableDataSource(rows);
  //     this.dataSource.paginator = this.paginator;
  //     this.dataSource.sort = this.sort;
  //   });
  // }

  loadData(schema: any, table: any): void {
  this.formService.getAll(schema, table).subscribe((rows: any[]) => {
    this.rows = rows;
    this.dataSource = new MatTableDataSource(rows);

    // ⭐ FIX: Attach paginator AFTER view updates
    setTimeout(() => {
      this.dataSource.paginator = this.paginator;
      this.dataSource.sort = this.sort;
    });
  });
}


  onEdit(row: any): void {
    this.selectedRow = row;
    this.form.patchValue(row);
    this.editRow(row);
  }

  editRow(row: any) {
    this.isEdit = true;
    const pkCol = this.columns.find(c => c.primaryKey);
    if (pkCol) {
      this.rowId = row[pkCol.name];
    }
    this.form.patchValue(row);
  }
  
  onSubmit() {
  if (!this.form.valid) return;

  const formData = this.form.getRawValue(); // include disabled fields like PK & audit fields

  // normalize date/timestamp fields
  this.columns.forEach(col => {
    const colType = (col.type || '').toLowerCase();
    if (!formData[col.name]) return;

    const val = formData[col.name];

    if (colType.includes('date') && !colType.includes('timestamp')) {
      const d = new Date(val);
      if (!isNaN(d.getTime())) formData[col.name] = d.toISOString().split('T')[0]; // yyyy-MM-dd
    } else if (colType.includes('timestamp')) {
      const d = new Date(val);
      if (!isNaN(d.getTime())) {
        const pad = (n: number) => n.toString().padStart(2, '0');
        formData[col.name] =
          `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T` +
          `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
      }
    }
  });

  const showAlert = (msg: string) => {
    // you asked for an alert emoji — ⚠️ for errors, ✅ for success
    window.alert(msg);
  };

  if (this.isEdit && this.rowId) {
    // UPDATE
    this.formService.update(this.selectedSchema, this.selectedTable, this.rowId, formData)
      .subscribe({
        next: (res: any) => {
          // backend may return 200 with body { status: 'error', message: '...' }
          if (res?.status === 'error') {
            showAlert(`⚠️ ${res.message || 'Operation failed'}`);
            return;
          }

          showAlert(`✅ Row updated successfully!`);
          this.isEdit = false;
          this.rowId = null;
          this.loadData(this.selectedSchema, this.selectedTable);
          this.form.reset();
        },
        error: (err: any) => {
          // err may be HttpErrorResponse with err.error as object/string
          const backendMsg =
            // common shapes
            err?.error?.message ||
            (typeof err?.error === 'string' ? err.error : null) ||
            err?.message ||
            `Update failed (status: ${err?.status ?? 'unknown'})`;

          showAlert(`⚠️ ${backendMsg}`);
        }
      });
  } else {
    // INSERT
    this.formService.insertRow(this.selectedSchema, this.selectedTable, formData)
      .subscribe({
        next: (res: any) => {
          // handle backend 200-with-error body
          if (res?.status === 'error') {
            showAlert(`⚠️ ${res.message || 'Insert failed'}`);
            return;
          }

          showAlert(`✅ Row inserted successfully!`);
          this.loadData(this.selectedSchema, this.selectedTable);
          this.form.reset();
        },
        error: (err: any) => {
          const backendMsg =
            err?.error?.message ||
            (typeof err?.error === 'string' ? err.error : null) ||
            err?.message ||
            `Insert failed (status: ${err?.status ?? 'unknown'})`;

          showAlert(`⚠️ ${backendMsg}`);
        }
      });
  }
}


  onDelete(row: any): void {
    if (confirm("Are you sure you want to delete this record?")) {
      const pkCol = this.columns.find(c => c.primaryKey);
      const rowId = row[pkCol.name]; 

      this.formService.delete(this.selectedSchema, this.selectedTable, rowId).subscribe({
        next: () => {
          alert('✅ Row deleted successfully!');
          this.loadData(this.selectedSchema, this.selectedTable);
        },
        error: (err) => {
          alert('Delete failed: ' + err.message);
        }
      });
    }
  }

  normalizeTimestamp(val: string): string {
    if (!val) return '';
    return val.replace(/\.\d+.*$/, '');
  }

  applyFilter(event: Event) {
    const filterValue = (event.target as HTMLInputElement).value.trim().toLowerCase();
    this.dataSource.filter = filterValue;
  }

  resetForm() {
    this.form.reset();
    this.selectedRow = null;
    this.isEdit = false;
    this.rowId = null;
  }

  goBack(): void {
    this.form.reset();
    this.isFormReady = false;
    this.selectedSchema = '';
    this.selectedTable = '';
    this.columns = [];
    this.dataSource.data = [];
    this.constraints = [];
    this.router.navigate(['/dynamic-form']);
  }

  // loadConstraints(schema: string, table: string): void {
  //   this.formService.getConstraints(schema, table).subscribe(res => {
  //     this.constraints = res;
  //   });
  // }
loadConstraints(schema: string, table: string): void {
  this.formService.getConstraints(schema, table).subscribe(res => {
    this.constraints = res;
    console.log("✅ Constraints fetched:", res);

    // 🔹 Call check constraint API for each column
    this.displayColumns.forEach(col => {
      this.formService.getCheckConstraintValues(`${schema}.${table}`, col.name)
        .subscribe(values => {
          console.log(`✅ Check constraint values for ${col.name}:`, values);
          if (values && values.length > 0) {
            this.checkOptions[col.name] = values;
            col.hasCheckConstraint = true;
          }
        });
    });
  });
}


  exportToExcel(): void {
    const data = this.dataSource.filteredData || this.dataSource.data;
    const worksheet = data.map(row => {
      const obj: any = {};
      this.columns.forEach(col => {
        obj[col.name] = row[col.name];
      });
      return obj;
    });
    

    const csvContent = this.convertToCSV(worksheet);
    this.downloadFile(csvContent, `${this.selectedTable}_data.csv`, 'text/csv');
  }

  exportToPDF(): void {
    window.print();
  }
 toDisplayString(value: any): string {
  if (value === null || value === undefined) return '';

  // ✅ If backend sends object like { type:'json', value:'"21"', null:false }
  if (typeof value === 'object' && value.value !== undefined) {
    try {
      // if value.value itself is a quoted string, unwrap it
      const inner = value.value;
      return typeof inner === 'string' && inner.startsWith('"') && inner.endsWith('"')
        ? inner.slice(1, -1)
        : inner;
    } catch {
      return String(value.value);
    }
  }

  // ✅ If it's a string that looks like JSON, parse it to clean it up
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      if (typeof parsed === 'string' || typeof parsed === 'number') {
        return String(parsed);
      }
    } catch {
      return value;
    }
  }

  // ✅ Fallback for any other type
  return String(value);
}


  exportToCSV(): void {
    const data = this.dataSource.filteredData || this.dataSource.data;
    const csvContent = this.convertToCSV(data);
    this.downloadFile(csvContent, `${this.selectedTable}_data.csv`, 'text/csv');
  }

  printTable(): void {
    window.print();
  }

  private convertToCSV(data: any[]): string {
    if (!data || data.length === 0) return '';
    
    const headers = this.columns.map(col => col.name).join(',');
    const rows = data.map(row => 
      this.columns.map(col => {
        const value = row[col.name] || '';
        return `"${String(value).replace(/"/g, '""')}"`;
      }).join(',')
    );
    
    return [headers, ...rows].join('\n');
  }

  private downloadFile(content: string, fileName: string, contentType: string): void {
    const blob = new Blob([content], { type: contentType });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = fileName;
    link.click();
    window.URL.revokeObjectURL(url);
  }
}