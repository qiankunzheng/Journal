#undef TRACEPOINT_PROVIDER
#define TRACEPOINT_PROVIDER mytest

#undef TRACEPOINT_INCLUDE
#define TRACEPOINT_INCLUDE "/root/src/ceph-0.87/src/mytest/tp.h"

#if !defined(_TP_H) || defined(TRACEPOINT_HEADER_MULTI_READ)
#define _TP_H

#include <lttng/tracepoint.h>

TRACEPOINT_EVENT(
	mytest,
	launch_op,
	TP_ARGS(int, id, const char*, value),
	TP_FIELDS(
	    ctf_integer(int, seq, id)
	    ctf_string(object, value)
	)
    )
TRACEPOINT_EVENT(
	mytest,
	finish_op,
	TP_ARGS(int, id, const char*, value),
	TP_FIELDS(
	    ctf_integer(int, seq, id)
	    ctf_string(object, value)
	)
)
#endif

#include <lttng/tracepoint-event.h>
