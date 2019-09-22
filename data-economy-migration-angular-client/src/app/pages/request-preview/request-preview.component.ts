import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

@Component({
    selector: 'app-request-preview',
    templateUrl: './request-preview.component.html',
    styleUrls: ['./request-preview.component.scss'],
    providers: []
})

export class RequestPreviewComponent implements OnInit {
    // bread crum data
    breadCrumbItems: Array<{}>;

    previewCols: any[];
    previewList: any = [];
    selectedRequestList: any = [];

    constructor(private router: Router) { }

    ngOnInit() {
        // tslint:disable-next-line: max-line-length
        this.breadCrumbItems = [{ label: 'Home', path: '/app/home' }, { label: 'Request', path: '/app/request' }, { label: 'Preview', active: true }];

        /**
         * fetch data
         */
        this._fetchData();

        this.previewCols = [
            { field: 'sno', header: 'Sr.No', width: '100px' },
            { field: 'dbName', header: 'DB Name', width: '180px' },
            { field: 'tableName', header: 'Table Name', width: '180px' },
            { field: 'filterCondition', header: 'Filter Condition', width: '180px' },
            { field: 'targetBucketName', header: 'Target Bucket Name', width: '100px' },
            { field: 'incrementalFlag', header: 'Incremental Flag', width: '100px' },
            { field: 'incrementalColumn', header: 'Incremental Column', width: '100px' }
        ];
    }

    /**
     * fetches the table value
     */
    _fetchData() {
        this.previewList = [{
            sno: '1',
            dbName: 'DB1',
            tableName: "Table1",
            filterCondition: 'Sample Condition',
            targetBucketName: 'Bucket1',
            incrementalFlag: false,
            incrementalColumn: "Col1"
        },
        {
            sno: '2',
            dbName: 'DB1',
            tableName: "Table2",
            filterCondition: 'Sample Condition',
            targetBucketName: 'Bucket1',
            incrementalFlag: false,
            incrementalColumn: "Col1"
        },
        {
            sno: '3',
            dbName: 'DB1',
            tableName: "Table3",
            filterCondition: 'Sample Condition',
            targetBucketName: 'Bucket1',
            incrementalFlag: false,
            incrementalColumn: "Col1"
        }]
    }

    onCancelFunction() {
        this.router.navigate(['/app/request']);
    }

    onContinueFunction() {
        this.router.navigate(['/app/basket']);
    }
}
