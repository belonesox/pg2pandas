#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This file is part of pg2pandas.
# https://github.com/belonesox/pg2pandas

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2018, Stas Fomin <stas-fomin@yandex.ru>

import itertools
import psycopg2
import numpy as np
import pandas.io.sql

#I need to get rid from django deps
from django.db import connection, transaction


from pandas.core.dtypes.common import (
    is_categorical_dtype,
    is_object_dtype,
    is_extension_type,
    is_extension_array_dtype,
    is_datetimetz,
    is_datetime64_any_dtype,
    is_bool_dtype,
    is_integer_dtype,
    is_float_dtype,
    is_integer,
    is_scalar,
    is_dtype_equal,
    needs_i8_conversion,
    _get_dtype_from_object,
    _ensure_float64,
    _ensure_int64,
    _ensure_platform_int,
    is_list_like,
    is_nested_list_like,
    is_iterator,
    is_sequence,
    is_named_tuple)

from pandas import compat
from pandas._libs import lib, algos as libalgos
from pandas.core.index import _ensure_index
import pandas.core.common as com


from pandas.core.internals import create_block_manager_from_arrays

import itertools
from pandas.core.dtypes.common import (
    is_categorical_dtype,
    is_object_dtype,
    is_extension_type,
    is_extension_array_dtype,
    is_datetimetz,
    is_datetime64_any_dtype,
    is_bool_dtype,
    is_integer_dtype,
    is_float_dtype,
    is_integer,
    is_scalar,
    is_dtype_equal,
    needs_i8_conversion,
    _get_dtype_from_object,
    _ensure_float64,
    _ensure_int64,
    _ensure_platform_int,
    is_list_like,
    is_nested_list_like,
    is_iterator,
    is_sequence,
    is_named_tuple)

from pandas.core.index import (Index, _ensure_index)
from pandas.core.frame import (extract_index)

from pandas import compat
import numpy as np
from pandas._libs import lib, algos as libalgos
import pandas.core.common as com


def _arrays_to_mgr(arrays, arr_names, index, columns, dtype=None):
    """
    Segregate Series based on type and coerce into matrices.
    Needs to handle a lot of exceptional cases.
    """
    # figure out the index, if necessary
    if index is None:
        index = extract_index(arrays)

    axes = [_ensure_index(columns), _ensure_index(index)]
    return create_block_manager_from_arrays(arrays, arr_names, axes)


def dataframe_from_sql(sql, con):
    """
    Read SQL query, return a DataFrame.

    Parameters
    ----------
    sql : SQL string with all parameters substituted
    con : connectable (django connection, or psycopg connection) 
    """
    
    # We have to estimate number of rows for one-time allocation of numpy arrays
    sql_count = "select count(*)  from (%s) s" % sql
    cursor = con.cursor()
    cursor.execute(sql_count)
    count = cursor.fetchone()[0]

    # Funny way to reliable get psycopg connection. We need it to get server-side cursors.
    pgcon = con.cursor().connection
    cursor.close()
    
    if count == 0:
        return None
    
    with transaction.atomic():
        cursor = pgcon.cursor("serversidecursor")
        cursor.execute(sql)
        
        chunk_size = min(count/10, 100000)
    
        columns = []
        arrays = []
    
        i = 0
        
        while True:
            rows = cursor.fetchmany(chunk_size)

            if not rows:
                break
    
            if not arrays:
                # Now we only support int/float types.
                # Todo: automatically build Category types from string fields
                for i, col_desc in enumerate(cursor.description):
                    columns.append(col_desc[0])
                    dtype = None
                    if col_desc.type_code == 700:
                        if col_desc.internal_size == 2:
                            dtype = np.float16
                        elif col_desc.internal_size == 2:
                            dtype = np.int16
                        elif col_desc.internal_size == 4:
                            dtype = np.float32
                        elif col_desc.internal_size == 8:
                            dtype = np.float64
                        else:
                            assert "Float with undefined length"
                    elif col_desc.type_code == psycopg2.NUMBER:
                        if col_desc.internal_size == 1:
                            dtype = np.int8
                        elif col_desc.internal_size == 2:
                            dtype = np.int16
                        elif col_desc.internal_size == 4:
                            dtype = np.int32
                        elif col_desc.internal_size == 8:
                            dtype = np.int64
                        else:
                            assert "Unknown number type"
            
                    thearray = np.zeros((count,), dtype=dtype)
                    arrays.append(thearray)
                columns = _ensure_index(columns)
                
            for row in rows:
                if i < count:
                    for j, thearray in enumerate(arrays):
                        if row[j]:
                            thearray[i] = row[j]
                i += 1
                
           #todo: resize arrays if  i < count
           # It possible, if result set changed between row count calculation,
           # of if we use random sampling in SQL select.

        cursor.close()
        del cursor

    mgr = _arrays_to_mgr(arrays, columns, None, columns)
    return pandas.core.api.DataFrame(mgr)