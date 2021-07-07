package com.ailbb.act.exception;

/*
 * Created by Wz on 8/31/2018.
 */
public class $KerberosException {
    /*
     * krb5文件不存在异常
     */
    public static class $Krb5NotFondExistsException extends Exception {

        public $Krb5NotFondExistsException() {
            super();
        }

        public $Krb5NotFondExistsException(String message) {
            super(message);
        }

        public $Krb5NotFondExistsException(String message, Throwable cause) {
            super(message, cause);
        }

        public $Krb5NotFondExistsException(Throwable cause) {
            super(cause);
        }
    }

}
