KEYS_DIR ?= keys
KEY_NAME ?= snowflake_rsa_key
SF_USER  ?= BANKING_PIPELINE_USER

.PHONY: snowflake-keys

# Unencrypted PKCS#8 key pair for Snowflake key-pair auth (git-ignored keys/).
# CI key: make snowflake-keys KEY_NAME=snowflake_ci_key SF_USER=BANKING_CI_USER
snowflake-keys:
	@mkdir -p $(KEYS_DIR) && chmod 700 $(KEYS_DIR)
	@if [ -f $(KEYS_DIR)/$(KEY_NAME).p8 ]; then \
		echo "$(KEYS_DIR)/$(KEY_NAME).p8 already exists; not overwriting it."; \
	else \
		openssl genrsa 2048 2>/dev/null | openssl pkcs8 -topk8 -inform PEM -nocrypt -out $(KEYS_DIR)/$(KEY_NAME).p8 && \
		chmod 600 $(KEYS_DIR)/$(KEY_NAME).p8 && \
		echo "Created $(KEYS_DIR)/$(KEY_NAME).p8"; \
	fi
	@openssl rsa -in $(KEYS_DIR)/$(KEY_NAME).p8 -pubout -out $(KEYS_DIR)/$(KEY_NAME).pub 2>/dev/null
	@echo "Run in Snowflake (as ACCOUNTADMIN):"
	@echo "ALTER USER $(SF_USER) SET RSA_PUBLIC_KEY = '$$(grep -v -- '-----' $(KEYS_DIR)/$(KEY_NAME).pub | tr -d '\n')';"
