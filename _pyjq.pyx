#cython: language_level=3
"""
Python binding for jq
"""

import os


class ScriptRuntimeError(Exception):
    """
    Exception thrown when a script calls error()
    """
    pass


cdef extern from "jv.h":
    ctypedef struct jv:
        pass
    const char* jv_string_value(jv)

    ctypedef enum jv_kind:
      JV_KIND_INVALID,
      JV_KIND_NULL,
      JV_KIND_FALSE,
      JV_KIND_TRUE,
      JV_KIND_NUMBER,
      JV_KIND_STRING,
      JV_KIND_ARRAY,
      JV_KIND_OBJECT


    jv jv_copy(jv)

    jv_kind jv_get_kind(jv)
    jv jv_invalid_get_msg(jv)
    int jv_invalid_has_msg(jv)

    void jv_free(jv)

    jv jv_invalid()
    jv jv_null()
    jv jv_bool(int)

    jv jv_number(double)
    double jv_number_value(jv)
    bint jv_is_integer(jv)

    jv jv_array()
    jv jv_array_sized(int)
    int jv_array_length(jv)
    jv jv_array_get(jv, int)
    jv jv_array_set(jv, int, jv)
    jv jv_array_append(jv, jv)

    jv jv_string_sized(const char*, int)
    jv jv_dump_string(jv, int) 

    jv jv_object()
    jv jv_object_get(jv object, jv key)
    jv jv_object_set(jv object, jv key, jv value)

    int jv_object_iter(jv)
    int jv_object_iter_next(jv, int)
    int jv_object_iter_valid(jv, int)
    jv jv_object_iter_key(jv, int)
    jv jv_object_iter_value(jv, int)

    ctypedef enum jv_parser_flags:
      JV_PARSE_EXPLODE_TOPLEVEL_ARRAY

    cdef struct jv_parser:
        pass

    jv_parser* jv_parser_new(int)
    int jv_parser_remaining(jv_parser*)
    void jv_parser_free(jv_parser*)
    void jv_parser_set_buf(jv_parser*, const char*, int, bint)
    jv jv_parser_next(jv_parser*)


cdef extern from "jq.h":
    ctypedef struct jq_state:
        pass

    ctypedef void (*jq_err_cb)(void *, jv)

    jq_state *jq_init()
    void jq_set_attr(jq_state *, jv, jv)
    void jq_teardown(jq_state **)
    bint jq_compile_args(jq_state *, const char* str, jv args)
    void jq_start(jq_state *, jv value, int flags)
    jv jq_next(jq_state *)
    void jq_set_error_cb(jq_state *, jq_err_cb, void *)


cdef object jv_to_pyobj(jv jval):
    kind = jv_get_kind(jval)

    if kind == JV_KIND_NULL:
        return None
    elif kind == JV_KIND_FALSE:
        return False
    elif kind == JV_KIND_TRUE:
        return True
    elif kind == JV_KIND_NUMBER:
        v = jv_number_value(jval)
        if jv_is_integer(jval):
            return int(v)
        return v
    elif kind == JV_KIND_STRING:
        return jv_string_value(jval).decode('utf-8')
    elif kind == JV_KIND_ARRAY:
        alist = []
        for i in range(jv_array_length(jv_copy(jval))):
            value = jv_array_get(jv_copy(jval), i)
            alist.append(jv_to_pyobj(value))
            jv_free(value)
        return alist
    elif kind == JV_KIND_OBJECT:
        adict = {}
        it = jv_object_iter(jval)
        while jv_object_iter_valid(jval, it):
            key = jv_object_iter_key(jval, it)
            k = jv_to_pyobj(key)
            jv_free(key)
            value = jv_object_iter_value(jval, it)
            v = jv_to_pyobj(value)
            jv_free(value)
            adict[k] = v
            it = jv_object_iter_next(jval, it)
        return adict


cdef jv pyobj_to_jv(object pyobj) except *:
    if isinstance(pyobj, str):
        pyobj = pyobj.encode('utf-8')
        return jv_string_sized(pyobj, len(pyobj))
    elif isinstance(pyobj, bytes):
        return jv_string_sized(pyobj, len(pyobj))
    elif isinstance(pyobj, bool):
        return jv_bool(pyobj)
    elif isinstance(pyobj, (int, long, float)):
        return jv_number(pyobj)
    elif isinstance(pyobj, (list, tuple)):
        jval = jv_array()
        for i, item in enumerate(pyobj):
            jval = jv_array_append(jval, pyobj_to_jv(item))
        return jval
    elif isinstance(pyobj, dict):
        jval = jv_object()
        for key, value in pyobj.items():
            if not isinstance(key, str):
                raise TypeError("Key of json object must be a str, but got {}".format(type(key)))
            jval = jv_object_set(jval, pyobj_to_jv(key), pyobj_to_jv(value))
        return jval
    elif pyobj is None:
        return jv_null()
    else:
        raise TypeError("{!r} could not be converted to json".format(type(pyobj)))


cdef void Script_error_cb(void* x, jv err):
    Script._error_cb(<object>x, err)


cdef jv_is_valid(jv value):
    kind = jv_get_kind(value)
    return kind != JV_KIND_INVALID

cdef class Script:
    'Compiled jq script object'
    cdef object _errors
    cdef jq_state* _jq

    def __init__(self, const char* script, vars={}, library_paths=[]):
        self._errors = []
        self._jq = jq_init()
        if not self._jq:
            raise RuntimeError('Failed to initialize jq')
        jq_set_error_cb(self._jq, Script_error_cb, <void*>self)

        args = pyobj_to_jv([
            dict(name=k, value=v)
            for k, v in vars.items()
        ])

        jq_set_attr(
            self._jq,
            pyobj_to_jv("JQ_LIBRARY_PATH"),
            pyobj_to_jv([str(path) for path in library_paths])
        )

        if not jq_compile_args(self._jq, script, args):
            raise ValueError("\n".join(self._errors))

    cdef _error_cb(self, jv err):
        self._errors.append(jv_string_value(err).decode('utf-8'))

    def __dealloc__(self):
        jq_teardown(&self._jq)

    def alli(self, iterable, slurp = False):
        # cdef jv slurped = jv_invalid()
        cdef jv_parser* parser = jv_parser_new(0)
        cdef bint is_last = False
        # cdef bint has_more = False
        cdef const char* c_buf

        while not is_last:
            if jv_parser_remaining(parser) == 0:
                try:
                    is_last = False
                    py_buf = next(iterable)
                except StopIteration:
                    is_last = True
                    py_buf = b''

                c_buf = py_buf
                jv_parser_set_buf(parser, c_buf, len(py_buf), not is_last)

            value = jv_parser_next(parser)
            if jv_is_valid(value):
                yield from process(self._jq, value)

    def all(self, pyobj):
        "Transform object by jq script, returning all results as list"
        cdef jv value = pyobj_to_jv(pyobj)
        return process(self._jq, value)

    apply = all

    def first(self, value, default=None):
        """
        Transform object by jq script, returning the first result.
        Return default if result is empty.
        """
        ret = self.apply(value)
        if not ret:
            return default
        return ret[0]

    def one(self, value):
        """
        Transform object by jq script, returning the first result.
        Raise IndexError unless results does not include exactly one element.
        """
        ret = self.apply(value)
        if not ret:
            raise IndexError("Result of jq is empty")
        elif len(ret) > 1:
            raise IndexError("Result of jq have multiple elements")
        return ret[0]

cdef list process(jq_state* jq, jv value):
    jq_start(jq, value, 0)
    cdef list output = []

    while True:
        result = jq_next(jq)
        try:
            kind = jv_get_kind(result)
            if kind == JV_KIND_INVALID:
                if not jv_invalid_has_msg(jv_copy(result)):
                    break
                m = jv_invalid_get_msg(jv_copy(result))
                e = str(jv_to_pyobj(m))
                raise ScriptRuntimeError(e)
            else:
                output.append(jv_to_pyobj(result))
        finally:
            jv_free(result)
    return output
