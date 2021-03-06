package com.ailbb.act.hadoop;

import com.ailbb.act.$;
import com.ailbb.act.kerberos.$Kerberos;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/*
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

    /*
     * 运行
     * @param action 接口对象
     * @param <T> 泛型
     * @return  结果集
     */
    public <T> T run(PrivilegedExceptionAction<T> action) throws Exception {
        if(!$.isEmptyOrNull(kerberos) && kerberos.isEnable() && kerberos.valid()) {
            $.info("执行Kerberos接口调用。");
            return  kerberos.getUgi().doAs(action);
        } else {
            $.info("执行普通接口调用。");
            return action.run();
        }
    }

}
