package com.cloud.project.config;

import com.owlike.genson.Genson;
import com.owlike.genson.GensonBuilder;
import com.owlike.genson.ext.jaxrs.GensonJaxRSFeature;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.ApplicationPath;

@ApplicationPath("/api")
public class AppConfig extends ResourceConfig {
    public AppConfig() {
        Genson genson = new GensonBuilder().setSkipNull(true).create();
        register(new GensonJaxRSFeature().use(genson));
        // if there are more than two packages then separate them with semicolon
        // exmaple : packages("org.foo.rest;org.bar.rest");
        packages("com.cloud.project.controller");
    }
}
