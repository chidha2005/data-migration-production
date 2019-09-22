import { Component, OnInit, ViewChild, AfterViewInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import 'rxjs/add/operator/filter';
import { Table } from 'primeng/table';

@Component({
    selector: 'app-history',
    templateUrl: './history.component.html',
    styleUrls: ['./history.component.scss'],
    providers: []
})

export class HistoryComponent implements OnInit, AfterViewInit {
    // bread crum data
    breadCrumbItems: Array<{}>;

    // Table data
    masterList: any[] = [];
    masterCols: any[];
    detailCols: any[];

    showMaster: boolean = true;
    showDetail: boolean = false;

    selectedRec: any;

    filterParams: any = {};

    @ViewChild("masterTable", { static: false }) masterTable: Table;

    constructor(private route: ActivatedRoute) { }

    ngAfterViewInit() {
        this.masterTable.filter(this.filterParams.status, "status", 'startsWith');
    }

    ngOnInit() {
        // tslint:disable-next-line: max-line-length
        this.breadCrumbItems = [{ label: 'Home', path: '/app/home' }, { label: 'History', active: true }];

        /**
         * fetch data
         */
        this._fetchData();

        this.masterCols = [
            { field: 'requestNo', header: 'Request No' },
            { field: 'requestedBy', header: 'Requested By' },
            { field: 'requestedTime', header: 'Requested Time' },
            { field: 'tokenizationFlag', header: 'Tokenization Enabled' },
            { field: 'status', header: 'Recon Status' },
            { field: 'requestType', header: 'Request Type' },
            { field: 'scriptGenCompletedTime', header: 'Script Generation Completed Time' },
            { field: 'executionCompletedTime', header: 'Execution Completed Time' }
        ];

        this.detailCols = [
            { field: 'sno', header: 'Sr.No' },
            { field: 'dbName', header: 'DB Name' },
            { field: 'tableName', header: 'Table Name' },
            { field: 'filterCondition', header: 'Filter Condition' },
            { field: 'targetBucketName', header: 'Target Bucket Name' },
            { field: 'incrementalFlag', header: 'Incremental Flag' },
            { field: 'incrementalColumn', header: 'Incremental Column' },
            { field: 'requestStatus', header: 'Request Status' }
        ];

        for (const key in this.masterCols) {
            this.filterParams[key] = "";
        }

        this.route.queryParams
            .filter(params => params.status)
            .subscribe(params => {
                this.filterParams.status = params["status"] || "";
            });

    }

    /**
     * fetches the table value
     */
    _fetchData() {
        this.masterList = [
            {
                requestNo: '1000',
                requestedBy: 'John Doe',
                requestedTime: '12/08/2019 16:00 EST',
                tokenizationFlag: true,
                status: 'Completed',
                requestType: 'HDFS to S3',
                scriptGenCompletedTime: '12/08/2019 16:10 EST',
                executionCompletedTime: '10 Min',
                details: [{
                    sno: '1',
                    dbName: 'DB1',
                    tableName: "Table1",
                    filterCondition: 'Sample Condition',
                    targetBucketName: 'Bucket1',
                    incrementalFlag: true,
                    incrementalColumn: false,
                    requestStatus: 'Completed'
                },
                {
                    sno: '2',
                    dbName: 'DB1',
                    tableName: "Table2",
                    filterCondition: 'Sample Condition',
                    targetBucketName: 'Bucket1',
                    incrementalFlag: true,
                    incrementalColumn: false,
                    requestStatus: 'In Progress'
                },
                {
                    sno: '3',
                    dbName: 'DB1',
                    tableName: "Table3",
                    filterCondition: 'Sample Condition',
                    targetBucketName: 'Bucket1',
                    incrementalFlag: true,
                    incrementalColumn: false,
                    requestStatus: 'Error'
                }]
            },
            {
                requestNo: '1001',
                requestedBy: 'John Doe',
                requestedTime: '12/08/2019 16:00 EST',
                tokenizationFlag: true,
                status: 'In Progress',
                requestType: 'HDFS to S3',
                scriptGenCompletedTime: '12/08/2019 16:10 EST',
                executionCompletedTime: '10 Min',
                details: [{
                    sno: '1',
                    dbName: 'DB1',
                    tableName: "Table1",
                    filterCondition: 'Sample Condition',
                    targetBucketName: 'Bucket1',
                    incrementalFlag: true,
                    incrementalColumn: false,
                    requestStatus: 'Completed'
                },
                {
                    sno: '2',
                    dbName: 'DB1',
                    tableName: "Table2",
                    filterCondition: 'Sample Condition',
                    targetBucketName: 'Bucket1',
                    incrementalFlag: true,
                    incrementalColumn: false,
                    requestStatus: 'In Progress'
                },
                {
                    sno: '3',
                    dbName: 'DB1',
                    tableName: "Table3",
                    filterCondition: 'Sample Condition',
                    targetBucketName: 'Bucket1',
                    incrementalFlag: true,
                    incrementalColumn: false,
                    requestStatus: 'Error'
                }]
            },
            {
                requestNo: '1002',
                requestedBy: 'John Doe',
                requestedTime: '12/08/2019 16:00 EST',
                tokenizationFlag: true,
                status: 'Error',
                requestType: 'HDFS to S3',
                scriptGenCompletedTime: '12/08/2019 16:10 EST',
                executionCompletedTime: '10 Min',
                details: [{
                    sno: '1',
                    dbName: 'DB1',
                    tableName: "Table1",
                    filterCondition: 'Sample Condition',
                    targetBucketName: 'Bucket1',
                    incrementalFlag: true,
                    incrementalColumn: false,
                    requestStatus: 'Completed'
                },
                {
                    sno: '2',
                    dbName: 'DB1',
                    tableName: "Table2",
                    filterCondition: 'Sample Condition',
                    targetBucketName: 'Bucket1',
                    incrementalFlag: true,
                    incrementalColumn: false,
                    requestStatus: 'In Progress'
                },
                {
                    sno: '3',
                    dbName: 'DB1',
                    tableName: "Table3",
                    filterCondition: 'Sample Condition',
                    targetBucketName: 'Bucket1',
                    incrementalFlag: true,
                    incrementalColumn: false,
                    requestStatus: 'Error'
                }]
            }
        ];
    }

    downloadTokenizationDetails() {

    }

    openReconDetails(selectedItem: any) {
        this.selectedRec = selectedItem;
        this.showMaster = false;
        this.showDetail = true;
    }

    cancelDetails() {
        this.selectedRec = null;
        this.showMaster = true;
        this.showDetail = false;
    }

    onDetailsSubmitFunction() {

    }

    onSubmitFunction() {

    }

    onCancelFunction() {

    }
}
