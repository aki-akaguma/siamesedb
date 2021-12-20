
TARGETS_64 = x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu mips64el-unknown-linux-muslabi64
TARGETS_32 = i686-unknown-linux-gnu armv7-unknown-linux-gnueabihf mipsel-unknown-linux-musl
TARGETS = $(TARGETS_64) $(TARGETS_32)

define template_target =
test-$(1):
	cargo test --target=$(1)
endef

$(foreach target,$(TARGETS_64),$(eval TEST_TARGETS_64=$(TEST_TARGETS_64) test-$(target)))
$(foreach target,$(TARGETS_32),$(eval TEST_TARGETS_32=$(TEST_TARGETS_32) test-$(target)))
TEST_TARGETS = $(TEST_TARGETS_64) $(TEST_TARGETS_32)

all: README.md

README.md: README.tpl src/lib.rs
	cargo readme > $@

test:
	cargo test

test64: $(TEST_TARGETS_64)

test32: $(TEST_TARGETS_32)

test-all: $(TEST_TARGETS)

test-no_std:
	cargo test --no-default-features

clean:
	@cargo clean
	@rm -f z.*

$(foreach target,$(TARGETS),$(eval $(call template_target,$(target))))
