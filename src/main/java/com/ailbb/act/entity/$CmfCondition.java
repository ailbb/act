package com.ailbb.act.entity;

import com.ailbb.ajj.entity.$Condition;

/**
 * Created by Wz on 8/23/2018.
 */
public class $CmfCondition extends $Condition {
    private String query;

    public String getQuery() {
        return query;
    }

    public $CmfCondition setQuery(String query) {
        this.query = query;
        return this;
    }
}
