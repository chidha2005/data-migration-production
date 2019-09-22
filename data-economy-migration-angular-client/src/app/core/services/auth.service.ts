import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

import { CookieService } from '../services/cookie.service';

@Injectable({ providedIn: 'root' })
export class AuthenticationService {
    user: any;

    constructor(private http: HttpClient, private cookieService: CookieService) {
    }

    /**
     * Returns the current user
     */
    public currentUser(): any {
        if (!this.user) {
            this.user = JSON.parse(this.cookieService.getCookie('currentUser'));
        }
        return this.user;
    }

    setUser(user: any) {
        this.user = user;
        this.cookieService.setCookie('currentUser', JSON.stringify(user), 1);
    }

    /**
     * Performs the auth
     * @param email email of user
     * @param password password of user
     */
    login(email: string, password: string) {
        return this.http.post<any>(`/api/login`, { email, password });
    }

    /**
     * Logout the user
     */
    logout() {
        // remove user from local storage to log user out
        this.cookieService.deleteCookie('currentUser');
        this.user = null;
    }
}

