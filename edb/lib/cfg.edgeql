#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


CREATE MODULE cfg;

CREATE ABSTRACT ANNOTATION cfg::backend_setting;
CREATE ABSTRACT ANNOTATION cfg::internal;
CREATE ABSTRACT ANNOTATION cfg::requires_restart;
CREATE ABSTRACT ANNOTATION cfg::system;

CREATE ABSTRACT TYPE cfg::ConfigObject EXTENDING std::BaseObject;

CREATE TYPE cfg::Port EXTENDING cfg::ConfigObject {
    CREATE REQUIRED PROPERTY port -> std::int64 {
        CREATE CONSTRAINT std::exclusive;
        SET readonly := true;
    };

    CREATE REQUIRED PROPERTY protocol -> std::str {
        SET readonly := true;
    };

    CREATE REQUIRED PROPERTY database -> std::str {
        SET readonly := true;
    };

    CREATE REQUIRED PROPERTY concurrency -> std::int64 {
        SET readonly := true;
    };

    CREATE REQUIRED PROPERTY user -> std::str {
        SET readonly := true;
    };

    CREATE REQUIRED MULTI PROPERTY address -> std::str {
        SET readonly := true;
        SET default := {'localhost'};
    };
};


CREATE ABSTRACT TYPE cfg::AuthMethod EXTENDING cfg::ConfigObject;
CREATE TYPE cfg::Trust EXTENDING cfg::AuthMethod;
CREATE TYPE cfg::SCRAM EXTENDING cfg::AuthMethod;


CREATE TYPE cfg::Auth EXTENDING cfg::ConfigObject {
    CREATE REQUIRED PROPERTY priority -> std::int64 {
        CREATE CONSTRAINT std::exclusive;
        SET readonly := true;
    };

    CREATE OPTIONAL MULTI PROPERTY user -> std::str {
        SET readonly := true;
        SET default := {'*'};
    };

    CREATE OPTIONAL SINGLE LINK method -> cfg::AuthMethod {
        CREATE CONSTRAINT std::exclusive;
    };

    CREATE OPTIONAL PROPERTY comment -> std::str {
        SET readonly := true;
    };
};


CREATE TYPE cfg::Config extending cfg::ConfigObject {
    CREATE REQUIRED PROPERTY listen_port -> std::int16 {
        CREATE ANNOTATION cfg::system := 'true';
        SET default := 5656;
    };

    CREATE REQUIRED MULTI PROPERTY listen_addresses -> std::str {
        CREATE ANNOTATION cfg::system := 'true';
    };

    CREATE OPTIONAL MULTI LINK ports -> cfg::Port {
        CREATE ANNOTATION cfg::system := 'true';
    };

    CREATE OPTIONAL MULTI LINK auth -> cfg::Auth {
        CREATE ANNOTATION cfg::system := 'true';
    };

    # Exposed backend settings follow.
    # When exposing a new setting, remember to modify
    # the _read_sys_config function to select the value
    # from pg_settings in the config_backend CTE.
    CREATE OPTIONAL PROPERTY shared_buffers -> std::str {
        CREATE ANNOTATION cfg::system := 'true';
        CREATE ANNOTATION cfg::backend_setting := '"shared_buffers"';
        CREATE ANNOTATION cfg::requires_restart := 'true';
        SET default := '-1';
    };

    CREATE OPTIONAL PROPERTY query_work_mem -> std::str {
        CREATE ANNOTATION cfg::backend_setting := '"work_mem"';
        SET default := '-1';
    };

    CREATE OPTIONAL PROPERTY effective_cache_size -> std::str {
        CREATE ANNOTATION cfg::backend_setting := '"effective_cache_size"';
        SET default := '-1';
    };

    CREATE OPTIONAL PROPERTY effective_io_concurrency -> std::str {
        CREATE ANNOTATION cfg::backend_setting := '"effective_io_concurrency"';
        SET default := '50';
    };

    CREATE OPTIONAL PROPERTY default_statistics_target -> std::str {
        CREATE ANNOTATION cfg::backend_setting := '"default_statistics_target"';
        SET default := '100';
    };
};


CREATE FUNCTION
cfg::get_config_json() -> std::json
{
    USING SQL $$
    SELECT jsonb_object_agg(cfg.name, cfg)
    FROM edgedb._read_sys_config() AS cfg
    $$;
};
