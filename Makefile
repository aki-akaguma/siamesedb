
all: readme

readme: README.md

README.md: README.tpl src/lib.rs
	cargo readme > $@

test:
	cargo test --offline

test-no-default-features:
	cargo test --offline --no-default-features

miri:
	cargo +nightly miri test --offline

clean:
	@cargo clean
	@rm -f z.*

clippy:
	cargo clippy --offline --tests --workspace

fmt:
	cargo fmt

doc:
	cargo doc

tarpaulin:
	cargo tarpaulin --offline --engine llvm --out html --output-dir ./target


FEATURES = --no-default-features --features=vf_vu64,key_cache,node_cache,buf_default
#FEATURES = --no-default-features --features=vf_u32u32,key_cache,node_cache,buf_default
#FEATURES = --no-default-features --features=vf_u64u64,key_cache,node_cache,buf_default

TARGETS_64 = x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu mips64el-unknown-linux-muslabi64
TARGETS_32 = i686-unknown-linux-gnu armv7-unknown-linux-gnueabihf mipsel-unknown-linux-musl
TARGETS = $(TARGETS_64) $(TARGETS_32)

define template_target =
test-$(1):
	cargo test $(FEATURES) --target=$(1)
endef

$(foreach target,$(TARGETS_64),$(eval TEST_TARGETS_64=$(TEST_TARGETS_64) test-$(target)))
$(foreach target,$(TARGETS_32),$(eval TEST_TARGETS_32=$(TEST_TARGETS_32) test-$(target)))
TEST_TARGETS = $(TEST_TARGETS_64) $(TEST_TARGETS_32)

test64: $(TEST_TARGETS_64)

test32: $(TEST_TARGETS_32)

test-all: $(TEST_TARGETS)

$(foreach target,$(TARGETS),$(eval $(call template_target,$(target))))
