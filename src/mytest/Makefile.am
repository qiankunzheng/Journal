if WITH_LTTNG
nodist_libmytest_tp_la_SOURCES = mytest/tp.h mytest/tp.c
endif
libmytest_tp_la_LIBADD = -llttng-ust -ldl
libmytest_tp_la_CPPFLAGS = -I$(tp_srcdir)/mytest/ -DTRACEPOINT_PROBE_DYNAMIC_LINKAGE
libmytest_tp_la_LDFLAGS =

if WITH_LTTNG
noinst_LTLIBRARIES += libmytest_tp.la
BUILT_SOURCES += mytest/tp.h
endif

GTEST_CXXFLAGS= \
	$(AM_CXXFLAGS) \
	-I$(top_srcdir)/src/gtest/include \
	-I$(top_builddir)/src/gtest/include 
LIB_GTEST= \
	$(top_srcdir)/src/gtest/lib/libgtest.a \
	$(top_srcdir)/src/gtest/lib/libgtest_main.a \
	$(PTHREAD_LIBS)

th_test_workload_SOURCES = mytest/mystore_test_workload.cc
th_test_workload_LDADD = $(LIBOS) $(CEPH_GLOBAL) $(LIB_GTEST)
if WITH_LTTNG
th_test_workload_LDADD += libmytest_tp.la
endif
th_test_workload_CXXFLAGS =  $(GTEST_CXXFLAGS)
noinst_PROGRAMS += th_test_workload
