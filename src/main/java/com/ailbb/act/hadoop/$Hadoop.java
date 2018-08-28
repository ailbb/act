package com.ailbb.act.hadoop;

import com.ailbb.act.$;
import com.ailbb.act.kerberos.$Kerberos;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * Created by Wz on 8/24/2018.
 */
public class $Hadoop {
    private $Kerberos kerberos;

    public $Kerberos getKerberos() {
        return kerberos;
    }

    public $Hadoop setKerberos($Kerberos kerberos) {
        this.kerberos = kerberos;
        return this;
    }

    /**
     * 运行
     * @param action
     * @param <T>
     * @return
     * @throws Exception
     */
    public <T> T run(PrivilegedExceptionAction<T> action) throws Exception {
        return ($.isEmptyOrNull(kerberos) || ! kerberos.valid()) ? action.run() : kerberos.getUgi().doAs(action);
    }

}
