package com.cloud.project.config;

import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.ApplicationPath;

@ApplicationPath("/api")
public class AppConfig extends ResourceConfig {
    public AppConfig() {
        // if there are more than two packages then separate them with semicolon
        // exmaple : packages("org.foo.rest;org.bar.rest");
        packages("com.cloud.project.controller");
    }
}
