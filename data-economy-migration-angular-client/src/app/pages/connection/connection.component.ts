import { Component, OnInit } from '@angular/core';

@Component({
    selector: 'app-connection',
    templateUrl: './connection.component.html',
    styleUrls: ['./connection.component.scss']
})


export class ConnectionComponent implements OnInit {

    breadCrumbItems: Array<{}>;

    constructor() { }

    private connectionModel: any = {
        "aswtoS3": {
            "connectionType": null,
            "longTermCredentials": {
                "accessID": "",
                "secretKey": ""
            },
            "shortTermCredentials": {
                "accessID": "",
                "secretKey": "",
                "roleARN": "",
                "principleARN": "",
                "samlProviderARN": "",
                "roleSessionName": "",
                "policyARNNumbers": "",
                "externalID": "",
                "federatedUserName": "",
                "inlineSessionPolicy": "",
                "duration": "",
                "ldapUserName": "",
                "ldapPassword": "",
                "ldapDomain": ""
            }
        },
        "hdfs": {
            "connectionType": null,
            "hiveDirectConnection": {
                "isSelected": false,
                "hostName": "",
                "portName": ""
            },
            "hiveUsingImpalaConnection": {
                "isSelected": false,
                "hostName": "",
                "portName": ""
            },
            "sparkOnHiveConnection": {
                "isSelected": false,
                "sqlWarehouseDir": "",
                "hiveMetastoreURI": ""
            },
            "authentication": {
                "authenticationType": "",
                "securedConnection": {
                    "connectionType": "",
                    "ldapConnection": {
                        "userName": "",
                        "password": "",
                        "domain": ""
                    },
                    "kerberosConnection": {
                        "hostRealm": "",
                        "hostFQDN": "",
                        "serviceName": "",
                        "sslKeyStorePath": ""
                    }
                }
            }
        },
        "targetFileProps": {
            "targetFormat": "",
            "targetCompression": "",
            "textFormat": {
                "fieldDelimiter": ""
            }
        },
        "otherProps": {
            "tokenization": {
                "tokenizationFlag": "",
                "protegrityDirectoryPath": ""
            },
            "parallelism": {
                "noOfParallelCopyJobs": "",
                "noOfParallelUserRequests": ""
            },
            "hdfs": {
                "tempHiveDatabase": "",
                "tempHDFSDir": "",
                "hdfsEdgeNode": "",
                "hdfsUserName": "",
                "hdfsUserPEMFileLocation": ""
            }
        }
    }

    ngOnInit() {
        this.breadCrumbItems = [{ label: 'Home', path: '/app/home' }, { label: 'Settings', active: true }, { label: 'Connection', active: true }];
    }

    cancelConnection(connectionGroup) {

    }

    testConnection(connectionGroup) {

    }

    saveConnection(connectionGroup) {

    }


}
