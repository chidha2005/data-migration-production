import { Component, OnInit } from '@angular/core';
import { ConfirmationService } from 'primeng/api';

@Component({
    selector: 'app-change-password',
    templateUrl: './change-password.component.html',
    styleUrls: ['./change-password.component.scss']
})


export class ChangePasswordComponent implements OnInit {

    breadCrumbItems: Array<{}>;

    changePasswordModel: any = {
        "newPassword": "",
        "confirmPassword": ""
    }

    constructor(private confirmationService: ConfirmationService) { }

    ngOnInit() {

        this.breadCrumbItems = [{ label: 'Home', path: '/app/home' }, { label: 'Settings', active: true }, { label: 'Change Password', active: true }];

    }

    cancelClickFunction() {

    }
    updateClickFunction() {

    }

}
