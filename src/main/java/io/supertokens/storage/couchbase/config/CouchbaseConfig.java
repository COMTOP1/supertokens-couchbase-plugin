/*
 *    Copyright (c) 2020, VRAI Labs and/or its affiliates. All rights reserved.
 *
 *    This software is licensed under the Apache License, Version 2.0 (the
 *    "License") as published by the Apache Software Foundation.
 *
 *    You may not use this file except in compliance with the License. You may
 *    obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *    License for the specific language governing permissions and limitations
 *    under the License.
 *
 */

package io.supertokens.storage.couchbase.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.supertokens.pluginInterface.exceptions.InvalidConfigException;

import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CouchbaseConfig {

    @JsonProperty
    private int couchbase_config_version = -1;

    @JsonProperty
    private String couchbase_connection_uri = null;

    @JsonProperty
    private String couchbase_user = null;

    @JsonProperty
    private String couchbase_password = null;

    @JsonProperty
    private String couchbase_bucket_name = null;

    @JsonProperty
    private String couchbase_key_value_collection_name = null;

    @JsonProperty
    private String couchbase_session_info_collection_name = null;

    @JsonProperty
    private String couchbase_collection_names_prefix = "";

    public static Set<String> getValidFields() {
        io.supertokens.storage.couchbase.config.CouchbaseConfig config = new io.supertokens.storage.couchbase.config.CouchbaseConfig();
        JsonObject configObj = new GsonBuilder().serializeNulls().create().toJsonTree(config).getAsJsonObject();

        Set<String> validFields = new HashSet<>();
        for (Map.Entry<String, JsonElement> entry : configObj.entrySet()) {
            validFields.add(entry.getKey());
        }
        return validFields;
    }

    public String getConnectionScheme() {
        URI uri = URI.create(couchbase_connection_uri);

        // sometimes if the scheme is missing, the host is returned as the scheme. To prevent that,
        // we have a check
        String host = this.getHostName();
        if (uri.getScheme() != null && !uri.getScheme().equals(host)) {
            return uri.getScheme();
        }
        return "couchbase";
    }

//    public String getConnectionAttributes() {
//        URI uri = URI.create(couchbase_connection_uri);
//        String query = uri.getQuery();
//        if (query != null) {
//            return query;
//        }
//        return "";
//    }

    public String getHostName() {
        URI uri = URI.create(couchbase_connection_uri);
        if (uri.getHost() != null) {
            return uri.getHost();
        }
        return "localhost";
    }

    public int getPort() {
        URI uri = URI.create(couchbase_connection_uri);
        return uri.getPort();
    }

    public String getUser() {
        if (couchbase_user.isEmpty())
            return "Administrator";
        return couchbase_user;
    }

    public String getPassword() {
        if (couchbase_password.isEmpty())
            return "password";
        return couchbase_password;
    }

    public String getBucketName() {
        return Objects.requireNonNullElse(couchbase_bucket_name, "supertokens");
    }

    public String getConnectionURI() {
        return couchbase_connection_uri;
    }

    public String getKeyValueCollection() {
        String tableName = "key_value";
        if (couchbase_key_value_collection_name != null) {
            return couchbase_key_value_collection_name;
        }
        return addPrefixToTableName(tableName);
    }

    public String getSessionInfoCollection() {
        String tableName = "session_info";
        if (couchbase_session_info_collection_name != null) {
            return couchbase_session_info_collection_name;
        }
        return addPrefixToTableName(tableName);
    }

    public String getJWTSigningKeysCollection() {
        return addPrefixToTableName("jwt_signing_keys");
    }

    private String addPrefixToTableName(String tableName) {
        if (!couchbase_collection_names_prefix.trim().isEmpty()) {
            return couchbase_collection_names_prefix.trim() + "_" + tableName;
        }
        return tableName;
    }

    void validateAndInitialise() throws InvalidConfigException {

        if (couchbase_connection_uri == null) {
            throw new InvalidConfigException(
                    "'couchbase_connection_uri' is not set in the config.yaml file. Please set this value and restart "
                            + "SuperTokens");
        }

        try {
            URI ignored = URI.create(couchbase_connection_uri);
        } catch (Exception e) {
            throw new InvalidConfigException(
                    "The provided couchbase connection URI has an incorrect format. Please use a format like "
                            + "couchbase+srv://[user[:[password]]@]host[:port][/dbname][?attr1=val1&attr2=val2...");
        }
    }
}