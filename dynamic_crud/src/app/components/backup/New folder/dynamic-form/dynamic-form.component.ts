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

  // @Input() table!: string;
  // @Input() schema!: string;

  // schema = 'hmfh';
  // table = 'ref_assessment_year';
  columns: any[] = [];
  rows: any[] = [];
  form!: FormGroup;
  selectedRow: any = null;
  isFormReady = false;

  isEdit: boolean = false;
  rowId: any = null;

  dataSource = new MatTableDataSource<any>();
  displayedColumns: string[] = [];

  schemaForm!: FormGroup;
  // schema: string = '';
  // table: string = '';
  
  schemas: string[] = [];
  tablesMap: { [schema: string]: string[] } = {};
  tables: string[] = [];

  selectedSchema: string = '';
  selectedTable: string = '';

  fkOptions: { [key: string]: any[] } = {};
  filteredFkOptions: { [key: string]: any[] } = {};

  constraints: any[] = [];

  @ViewChild(MatPaginator) paginator!: MatPaginator;
  @ViewChild(MatSort) sort!: MatSort;

  constructor(private formService: DynamicFormService, private fb: FormBuilder) {}

  ngOnInit(): void {
    this.schemaForm = this.fb.group({
      schema: ['', Validators.required],
      table: ['', Validators.required]
    });
    //  this.loadMetadata();
    //  this.loadData();
    this.formService.getAllTables().subscribe(data => {
      this.tablesMap = data;
      this.schemas = Object.keys(data); // all schema names
    });
  }

  onSchemaChange() {
    const schema = this.schemaForm.value.schema;
    this.tables = this.tablesMap[schema] || [];
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

  loadMetadata(schema:any, table:any): void {
    this.formService.getColumns(schema, table).subscribe(cols => {
      this.columns = cols;

      // Reset FK options to avoid duplicates
      this.fkOptions = {};

      // fetch FK values
      this.columns.forEach(col => {
        if (col.isForeignKey) {
          this.formService.getForeignKeyValues(schema, table, col.name)
            .subscribe(values => this.fkOptions[col.name] = values);
        }
      });

      const group: { [key: string]: any } = {};
      this.columns.forEach(col => {
        const validators = col.nullable ? [] : [Validators.required];
      
        let value = '';
      
        if (col.type.toLowerCase().includes('timestamp')) {
          value = this.normalizeTimestamp(col.defaultValue || '');
        }
      
        group[col.name] = this.fb.control({ value: value, disabled: col.autoIncrement || col.primaryKey },validators);
      });

      this.form = this.fb.group(group);
      this.isFormReady = true;
      this.displayedColumns = [...this.columns.map(c => c.name), 'actions'];
    });

    this.loadConstraints(schema, table);
  }

  onFilterFk(colName: string, event: Event): void {
    const filterValue = (event.target as HTMLInputElement).value.toLowerCase();
    // this.filteredFkOptions[colName] = this.fkOptions[colName].filter(option =>
    //   option.value.toLowerCase().includes(filterValue)
    // );
    this.filteredFkOptions[colName] = this.fkOptions[colName].filter(option =>
      option.value.toLowerCase().startsWith(filterValue)   // 🔑 only match from beginning
    );
  }

  loadData(schema:any, table:any): void {
    //this.formService.getAll(this.schema, this.table).subscribe((rows : any[]) => {
    this.formService.getAll(schema, table).subscribe((rows : any[]) => {
      this.rows = rows;
      //this.dataSource.data = rows;
      this.dataSource = new MatTableDataSource(rows);
      this.dataSource.paginator = this.paginator;
      this.dataSource.sort = this.sort;
    });
  }

  onEdit(row: any): void {
    this.selectedRow = row;
    this.form.patchValue(row);
    this.editRow(row);
  }

  // onSubmit(): void {
  //   if (this.selectedRow) {
  //     // update
  //     const id = this.selectedRow.id || this.selectedRow.emp_id; // adjust based on PK
  //     this.formService.update(this.schema, this.table, id, this.form.value).subscribe(() => {
  //       this.loadData();
  //       this.form.reset();
  //       this.selectedRow = null;
  //     });
  //   } else {
  //     // create
  //     this.formService.create(this.schema, this.table, this.form.value).subscribe(() => {
  //       this.loadData();
  //       this.form.reset();
  //     });
  //   }
  // }

  editRow(row: any) {
    this.isEdit = true;
    //this.rowId = row.id; // 👈 use the actual PK column name (e.g., "emp_id")
    // 👇 Get the primary key column (assuming your API sends primaryKey=true in column metadata)
    const pkCol = this.columns.find(c => c.primaryKey);
    if (pkCol) {
      this.rowId = row[pkCol.name];  // store primary key value
    }
  
    // populate form with row data
    this.form.patchValue(row);
  }
  
  onSubmit() {
    // if (this.form.valid) {
    //   this.formService.insertRow(this.schema, this.table, this.form.value).subscribe({
    //     next: (res) => {
    //       alert('Row inserted successfully!');
    //       console.log(res);
    //     },
    //     error: (err) => {
    //       alert('Insert failed: ' + err.message);
    //     }
    //   });
    // }
    if (this.form.valid) {
      const formData = this.form.getRawValue(); // include disabled fields like PK
      console.log('formData--'+formData);
  
      console.log('isEdit--->'+this.isEdit);
      console.log('this.rowId--->'+this.rowId);

      this.columns.forEach(col => {
        const colType = col.type.toLowerCase();

      if (formData[col.name]) {
        const val = formData[col.name];

        // ✅ Handle DATE columns
        if (colType.includes("date") && !colType.includes("timestamp")) {
          const d = new Date(val);
          formData[col.name] = d.toISOString().split("T")[0]; // yyyy-MM-dd
        }

        // ✅ Handle TIMESTAMP columns
        else if (colType.includes("timestamp")) {
          const d = new Date(val);
          const pad = (n: number) => n.toString().padStart(2, "0");
          formData[col.name] =
            `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T` +
            `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
        }
      }


        // if (col.type.toLowerCase().includes('timestamp') && formData[col.name]) {
        //   // Convert JS Date → yyyy-MM-ddTHH:mm:ss
        //   const date = new Date(formData[col.name]);
        //   formData[col.name] = date.toISOString().slice(0, 19); 
        // }
      });

      if (this.isEdit && this.rowId) {
        console.log('update');
        // 🔹 UPDATE
        this.formService.update(this.selectedSchema, this.selectedTable, this.rowId, formData).subscribe({
          next: (res) => {
            alert('Row updated successfully!');
            console.log(res);
            this.isEdit = false;
          this.rowId = null;

          // 🔄 Refresh table data
          this.loadData(this.selectedSchema, this.selectedTable);

          // Reset form if needed
          this.form.reset();
          },
          error: (err) => {
            alert('Update failed: ' + err.message);
          }
        });
      } else {
        console.log('Insert');
        // 🔹 INSERT
        this.formService.insertRow(this.selectedSchema, this.selectedTable, formData).subscribe({
          next: (res) => {
            alert('Row inserted successfully!');
            console.log(res);
            this.loadData(this.selectedSchema, this.selectedTable);

          // Reset form
          this.form.reset();
          },
          error: (err) => {
            alert('Insert failed: ' + err.message);
          }
        });
      }
    }
    //this.loadData();
  }

  // onDelete(row: any): void {
  //   console.log('this.rowId--->'+this.rowId);
  //   const id = row.id || row.emp_id; // adjust PK column
  //   this.formService.delete(this.selectedSchema, this.selectedTable, id).subscribe(() => {
  //     this.loadData(this.selectedSchema, this.selectedTable);
  //   });
  // }

  onDelete(row: any): void {
    if (confirm("Are you sure you want to delete this record?")) {
      // const schema = this.schema;
      // const table = this.table;
  
      // Assuming your backend API expects primary key value as {id}
      const pkCol = this.columns.find(c => c.primaryKey);
      console.log('pkCol--->'+pkCol);
      console.log('pkCol.name--->'+pkCol.name);
      const rowId = row[pkCol.name]; 
      console.log('rowId--->'+rowId);
  
      this.formService.delete(this.selectedSchema, this.selectedTable, rowId).subscribe({
        next: () => {
          alert('Row deleted successfully!');
          this.loadData(this.selectedSchema, this.selectedTable); // ✅ refresh after delete
        },
        error: (err) => {
          alert('Delete failed: ' + err.message);
        }
      });
    }
  }

  normalizeTimestamp(val: string): string {
    if (!val) return '';
    // remove timezone and milliseconds if present
    return val.replace(/\.\d+.*$/, '');
  }

  applyFilter(event: Event) {
    const filterValue = (event.target as HTMLInputElement).value.trim().toLowerCase();
    this.dataSource.filter = filterValue;
  }

  resetForm() {
    this.form.reset();
    this.selectedRow = null;   // clear edit mode
    this.isEdit = false;       // reset flag if you are in edit mode
    this.rowId = null;         // reset PK reference
  }

  goBack(): void {
    this.isFormReady = false;   // Hide form + table
    this.selectedSchema = ''; // Reset selected schema
    this.selectedTable = '';  // Reset selected table
    this.columns = [];
    this.dataSource.data = [];
  }

  loadConstraints(schema: string, table: string): void {
    this.formService.getConstraints(schema, table).subscribe(res => {
      this.constraints = res;
      console.log("Constraints loaded:", this.constraints);
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
    
    // Create CSV content
    const csvContent = this.convertToCSV(worksheet);
    this.downloadFile(csvContent, `${this.selectedTable}_data.csv`, 'text/csv');
  }

  exportToPDF(): void {
    window.print();
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
