package com.ailbb.act.exception;

/**
 * Created by Wz on 8/31/2018.
 */
public class $HBaseException {
    /**
     * 表存在异常
     */
    public static class $TableExistsException extends Exception {

        public $TableExistsException() {
            super();
        }

        public $TableExistsException(String message) {
            super(message);
        }

        public $TableExistsException(String message, Throwable cause) {
            super(message, cause);
        }

        public $TableExistsException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * 表不存在异常
     */
    public static class $TableNotExistsException extends Exception {

        public $TableNotExistsException() {
            super();
        }

        public $TableNotExistsException(String message) {
            super(message);
        }

        public $TableNotExistsException(String message, Throwable cause) {
            super(message, cause);
        }

        public $TableNotExistsException(Throwable cause) {
            super(cause);
        }
    }
}
