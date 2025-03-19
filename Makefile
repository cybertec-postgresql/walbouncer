PG_CONFIG = pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

all:

installcheck:
	WALBOUNCER=walbouncer PATH=$(DESTDIR)$(bindir):$(bindir):$(PATH) tests/run_demo.sh

all install clean:
	$(MAKE) -C src $@
