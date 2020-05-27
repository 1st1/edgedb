#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


abstract annotation user_anno;
abstract inheritable annotation heritable_user_anno;

function user_func_0(x: int64) -> str {
    using (
        SELECT 'func' ++ <str>x
    );
    annotation title := 'user_func(int64) -> str';
    volatility := 'IMMUTABLE';
};

function user_func_1(x: array<int64>, y: str) -> str {
    using (
        SELECT array_join(<array<str>>x, y)
    );
    volatility := 'IMMUTABLE';
};

function user_func_2(x: OPTIONAL int64, y: str = 'x') -> SET OF str {
    using (
        SELECT {<str>x, y}
    );
    volatility := 'IMMUTABLE';
};

type A {
    annotation title := 'A';

    optional property p_bool -> bool {
        annotation title := 'single bool';
    }
    optional property p_str -> str;
    optional property p_datetime -> datetime;
    optional property p_local_datetime -> cal::local_datetime;
    optional property p_local_date -> cal::local_date;
    optional property p_local_time -> cal::local_time;
    optional property p_duration -> duration;
    optional property p_int16 -> int16;
    optional property p_int32 -> int32;
    optional property p_int64 -> int64;
    optional property p_float32 -> float32;
    optional property p_float64 -> float64;
    optional property p_bigint -> bigint;
    optional property p_decimal -> decimal;
    optional property p_json -> json;
    optional property p_bytes -> bytes;
}


type B {
    annotation title := 'B';

    required multi property p_bool -> bool {
        annotation title := 'multi bool';
    }
    required multi property p_str -> str;
    required multi property p_datetime -> datetime;
    required multi property p_local_datetime -> cal::local_datetime;
    required multi property p_local_date -> cal::local_date;
    required multi property p_local_time -> cal::local_time;
    required multi property p_duration -> duration;
    required multi property p_int16 -> int16;
    required multi property p_int32 -> int32;
    required multi property p_int64 -> int64;
    required multi property p_float32 -> float32;
    required multi property p_float64 -> float64;
    required multi property p_bigint -> bigint;
    required multi property p_decimal -> decimal;
    required multi property p_json -> json;
    required multi property p_bytes -> bytes;
}


type C {
    annotation title := 'C';
    required property val -> str {
        annotation title := 'val';
        constraint exclusive {
            annotation title := 'exclusive C val';
        }
    }
}


type D {
    annotation title := 'D';
    annotation user_anno := 'D only';
    annotation heritable_user_anno := 'all D';

    required property num -> int64;

    optional link single_link -> C {
        annotation title := 'optional single link to C';
    }
    optional multi link multi_link -> C {
        annotation title := 'optional multi link to C';
    }
}


type E extending D {
    annotation title := 'E';

    overloaded optional link single_link -> C {
        optional property lp0 -> str {
            annotation title := 'single lp0';
        }
    }
    overloaded optional multi link multi_link -> C {
        optional property lp1 -> str {
            annotation title := 'single lp1';
        }
    }
}


type F extending D {
    annotation title := 'F';

    overloaded required link single_link -> C;
    overloaded required multi link multi_link -> C;
}


type G {
    required property g0 -> str {
        default := 'fixed';
    };
    required property g1 -> str {
        default := user_func_0(1);
    };
    required property g2 -> str {
        default := to_str(2);
    };
}


type H {
    optional property h0 := 'fixed';
    optional property h1 := user_func_0(1);
    optional property h2 := to_str(2);
}


type I {
    required link i0 -> C {
        default := (SELECT C FILTER .val = 'D00' LIMIT 1);
    };
    required link i1 -> C {
        default := (
            SELECT C FILTER .val = 'D0' ++ user_func_0(1)[-1] LIMIT 1
        );
    };
    required link i2 -> C {
        default := (
            SELECT C FILTER .val = array_join(['D', '0', '2'], '') LIMIT 1
        );
    };
}


type J {
    optional link j0 := (SELECT C FILTER .val = 'D00' LIMIT 1);
    optional link j1 := (
        SELECT C FILTER .val = 'D0' ++ user_func_0(1)[-1] LIMIT 1
    );
    optional link j2 := (
        SELECT C FILTER .val = array_join(['D', '0', '2'], '') LIMIT 1
    );
}


# indexes
type K {
    required property k -> str;
    index on (.k);
}


type L {
    required property l0 -> str;
    required property l1 -> str;
    index on (.l0 ++ .l1);
}


# constraints
abstract constraint user_int_constr(x: int64) {
    using (__subject__ > x);
    errmessage := '{__subject__} must be greater than {x}';
    annotation title := 'user_int_constraint constraint'
}

scalar type UserInt extending int64 {
    annotation title := 'UserInt scalar';
    constraint user_int_constr(5);
}


scalar type UserStr extending str {
    annotation title := 'UserStr scalar';
    constraint max_len_value(5);
}


type M {
    required property m0 -> int64 {
        constraint user_int_constr(3);
    }
    required property m1 -> str {
        constraint max_len_value(3);
    }
}


type N {
    required property n0 -> UserInt;
    required property n1 -> UserStr;
}


# enum
scalar type UserEnum extending enum<'Lorem', 'ipsum', 'dolor', 'sit', 'amet'>;


type O {
    required property o0 -> UserEnum;
    required property o1 -> UserEnum {
        default := <UserEnum>'Lorem';
    };
    optional property o2 := <UserEnum>'dolor';
}


# collection props
type P {
    required link plink0 -> C {
        optional property p0 -> array<str>;
    };
    required link plink1 -> C {
        optional property p1 -> array<float64>;
    };
    required property p2 -> array<str>;
    required property p3 -> array<float64>;
}


type Q {
    required property q0 -> tuple<int64, bool>;
    required property q1 -> tuple<str, decimal>;
    required property q2 -> tuple<x: int64, y: bool>;
    required property q3 -> tuple<x: str, y: decimal>;
}


# inheritance and delegated constraint
abstract type R {
    required property name -> str {
        delegated constraint exclusive;
    }
}


type S extending R {
    required property s -> str;
}


type T extending R {
    required property t -> str;
}


abstract type U {
    required property u -> str;
}


type V extending U, S, T;


# aliases
alias AliasP := P {
    name := 'alias P',
    p2 := .p2 ++ ['!'],
    f := F {
        k := (SELECT K LIMIT 1)
    }
};


alias Primes := {2, 3, 5, 7};


# self-referential and mutually-referential types
type W {
    required property name -> str {
        constraint exclusive;
    }
    optional link w -> W;
}


type X {
    required property name -> str;
    optional link y -> Y;
}


type Y {
    required property name -> str;
    optional link x -> X;
}


# link target as a union type
type Z {
    # have only 'id' in common
    optional link ck -> C | K;
    # have 'name' in common
    optional multi link stw -> S | T | W;
}


# cross-module references
type DefA extending test::TestA;


type DefB {
    required property name -> str {
        default := test::user_func_3(0);
    }
    optional link other -> test::TestB;
}


type DefC {
    required property name -> str {
        default := test::user_func_3(1);
    }
    optional link other -> test::TestC;
}


# on-target delete restrictions
type TargetA {
    required property name -> str {
        constraint exclusive;
    }
}

type SourceA {
    required property name -> str {
        constraint exclusive;
    }

    optional link link0 -> TargetA {
        on target delete restrict;
    };
    optional link link1 -> TargetA {
        on target delete delete source;
    };
    optional link link2 -> TargetA {
        on target delete allow;
    };
    optional link link3 -> TargetA {
        on target delete deferred restrict;
    };
};


# read-only links and props
type ROPropsA {
    required property name -> str {
        constraint exclusive;
    }

    optional property rop0 -> int64 {
        readonly := True;
    }
    required property rop1 -> int64 {
        readonly := True;
        default := <int64>round(10 * random());
    }
}


type ROLinksA {
    required property name -> str {
        constraint exclusive;
    }

    optional link rol0 -> C {
        readonly := True;
    }
    required link rol1 -> C {
        readonly := True;
        default := (SELECT C FILTER .val = 'D00');
    }
    required multi link rol2 -> C {
        readonly := True;
        default := (SELECT C FILTER .val IN {'D01', 'D02'});
    }
}


type ROLinksB {
    required property name -> str {
        constraint exclusive;
    }

    optional link rol0 -> C {
        optional property rolp00 -> int64 {
            readonly := True;
        }
        optional property rolp01 -> int64 {
            readonly := True;
            default := <int64>round(10 * random());
        }
    }
    optional multi link rol1 -> C {
        optional property rolp10 -> int64 {
            readonly := True;
        }
        optional property rolp11 -> int64 {
            readonly := True;
            default := <int64>round(10 * random());
        }
    }
}
