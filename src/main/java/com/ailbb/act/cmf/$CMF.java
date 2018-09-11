package com.ailbb.act.cmf;

import com.ailbb.act.$;
import com.ailbb.act.entity.$CmfCondition;
import com.ailbb.ajj.entity.$Condition;
import com.ailbb.ajj.entity.$ConnConfiguration;
import com.ailbb.ajj.entity.$Result;
import com.ailbb.ajj.http.Ajax;
import com.cloudera.api.ApiRootResource;
import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.model.ApiTimeSeriesRequest;
import com.cloudera.api.v11.RootResourceV11;
import com.cloudera.api.v11.TimeSeriesResourceV11;
import net.sf.json.JSONObject;
import org.apache.cxf.jaxrs.impl.ResponseImpl;

import javax.ws.rs.core.Response;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * Created by Wz on 8/22/2018.
 */
public class $CMF {
    private $ConnConfiguration connConfiguration;
    private ApiRootResource apiRootResource;
    public final int $PORT = 7180;

    /**
     * 初始化方法
     * @param connConfiguration 连接配置
     * @return 当前对象
     */
    public $CMF init($ConnConfiguration connConfiguration) throws Exception {
        $.info("============== CMF执行初始化 ==============");

        try {
            this.setConnConfiguration(connConfiguration).login();
        } finally {
            $.info("============== CMF初始化完成 ==============");
        }

        return this;
    }

    /**
     * 登录cm
     * @return 结果对象
     */
    public $Result login() {
        this.setApiRootResource(
                new ClouderaManagerClientBuilder()
                        .withHost(connConfiguration.getIp()).withPort(connConfiguration.getPort())
                        .withUsernamePassword(connConfiguration.getUsername(), connConfiguration.getPassword()).build()
        );

//        $.post(new Ajax(getURL("login")).setData(JSONObject.fromObject(new HashMap<String, String>(){{
//            put("j_username", connConfiguration.getUsername());
//            put("j_password", connConfiguration.getPassword());
//        }})));

        return $.result();
    }

    /**
     * 获取响应数据
     * @param response 数据对象
     * @return $Result 结构体
     */
    public $Result getResponseData(Response response){
        JSONObject jsonObject = JSONObject.fromObject(((ResponseImpl) response).readEntity(String.class));
        return $.result().setData(jsonObject.get("items"));
    }

    /**
     * 获取数据
     * @param condition $CmfCondition
     * @return $Result 结构体
     */
    public $Result queryTimeSeriesResponse($CmfCondition condition) {
        $Result rs = $.result();
        try {
            Date fromDate = $.date.parse(condition.getStarttime());
            Date toDate = $.date.parse(condition.getEndtime());
            String fromformat = $.date.format("yyyy-MM-dd'T'HH:mm:ss.SSS'+0800'", fromDate);
            String toformat = $.date.format("yyyy-MM-dd'T'HH:mm:ss.SSS'+0800'", toDate);

            long between = (toDate.getTime() - fromDate.getTime());
            int minutes = (int) (between / (1000 * 60));
            String desire = "RAW";

            if (minutes <= 30) {
                desire = "RAW";
            } else if (minutes > 30 && minutes < 300) {
                desire = "TEN_MINUTELY";
            } else if (minutes >= 300 && minutes < 1800) {
                desire = "HOURLY";
            } else if (minutes >= 1800 && minutes < 10800) {
                desire = "SIX_HOURLY";
            } else {
                desire = "DAILY";
            }

            ApiTimeSeriesRequest atsr = new ApiTimeSeriesRequest();
            atsr.setQuery(condition.getQuery());
            atsr.setFrom(fromformat);
            atsr.setTo(toformat);
            atsr.setDesiredRollup(desire);
            atsr.setMustUseDesiredRollup(true);

            RootResourceV11 v11 = apiRootResource.getRootV11();

            TimeSeriesResourceV11 t11 = v11.getTimeSeriesResource();
            Response res = t11.queryTimeSeries(atsr);

            rs.setData(rs);
        } catch (ParseException e) {
            rs.addMessage("时间格式化失败！").addError($.exception(e));
        }

        return rs;
    }

    /**
     * cmf请求资源链接
     * @param type 类型
     * @return url
     */
    public String getURL(String type){
        String controller = "";

        if(type.equalsIgnoreCase("login")) controller = "/j_spring_security_check";

        if(type.equalsIgnoreCase("host")) controller = "/cmf/hardware/hosts/hostsOverview.json";

        if(type.equalsIgnoreCase("resources")) controller = "/cmf/charts/timeSeries";

        if(type.equalsIgnoreCase("host_id")) controller = "/cmf/events/query";

        return $.url($.concat(connConfiguration.getIp(), ":", connConfiguration.getPort(), controller));
    }

    public ApiRootResource getApiRootResource() {
        return apiRootResource;
    }

    public $CMF setApiRootResource(ApiRootResource apiRootResource) {
        this.apiRootResource = apiRootResource;
        return this;
    }

    public $ConnConfiguration getConnConfiguration() {
        return connConfiguration;
    }

    public $CMF setConnConfiguration($ConnConfiguration connConfiguration) {
        this.connConfiguration = connConfiguration;
        return this;
    }
}
