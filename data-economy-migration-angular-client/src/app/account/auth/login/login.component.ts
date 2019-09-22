import { Component, OnInit, AfterViewInit } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { first } from 'rxjs/operators';

import { ActivatedRoute, Router } from '@angular/router';
import { NotificationService } from 'src/app/core/services/notification.service';
import { AuthenticationService } from 'src/app/core/services/auth.service';
import { User } from 'src/app/core/models/auth.models';

@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.scss']
})
export class LoginComponent implements OnInit, AfterViewInit {

  loginForm: FormGroup;
  submitted = false;
  returnUrl: string;
  error = '';
  loading = false;

  constructor(private formBuilder: FormBuilder, private route: ActivatedRoute, private router: Router,
    private authenticationService: AuthenticationService,
    private notificationService: NotificationService) { }

  ngOnInit() {
    this.loginForm = this.formBuilder.group({
      userName: ['', [Validators.required]],
      password: ['', Validators.required],
    });

    // reset login status
    this.authenticationService.logout();

    // get return url from route parameters or default to '/'
    // tslint:disable-next-line: no-string-literal
    this.returnUrl = this.route.snapshot.queryParams['returnUrl'] || 'app/home';
  }

  ngAfterViewInit() {
    document.body.classList.add('authentication-bg');
    document.body.classList.add('authentication-bg-pattern');
  }

  // convenience getter for easy access to form fields
  get f() { return this.loginForm.controls; }

  /**
   * On submit form
   */
  onSubmit() {
    this.submitted = true;

    // stop here if form is invalid
    if (this.loginForm.invalid) {
      return;
    }

    this.loading = true;
    // this.authenticationService.login(this.f.userName.value, this.f.password.value)
    //   .subscribe(
    //     data => {
    //       this.notificationService.showSuccess('User Logged in successfully');
    //       this.router.navigate([this.returnUrl]);
    //     },
    //     error => {
    //       this.error = error;
    //       this.loading = false;
    //     });
    let currentuser = new User();
    currentuser.email = "admin@admin.com";
    this.authenticationService.setUser(currentuser);
    this.notificationService.showSuccess('User Logged in successfully');
    this.router.navigate([this.returnUrl]);
  }
}
