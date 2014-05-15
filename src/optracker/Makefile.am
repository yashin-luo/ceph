if ENABLE_SYSTEMTAP
optracker/optracker_probes.h: optracker/optracker_probes.d
	$(DTRACE) -C -h -s $< -o $@
optracker/optracker_probes.o: optracker/optracker_probes.d
	$(DTRACE) -C -G -s $< -o $@

OPTRACKERPROBELDADD = optracker/optracker_probes.o
else
OPTRACKERPROBELDADD =
endif

liboptracker_la_SOURCES = \
	optracker/TrackedResource.cc \
	optracker/TrackedMutex.cc \
	optracker/TrackedOp.cc

if ENABLE_SYSTEMTAP
liboptracker_la_DEPS = optracker/optracker_probes.h optracker/optracker_probes.o
BUILT_SOURCES += optracker/optracker_probes.h
endif

noinst_LTLIBRARIES += liboptracker.la

liboptracker_la_LIBADD = $(LIBCOMMON)

noinst_HEADERS += \
	optracker/TrackedMutex.h \
	optracker/TrackedResource.h \
	optracker/TrackedOp.h

