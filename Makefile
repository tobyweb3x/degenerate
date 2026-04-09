BASE_NAME = bot_archive
ZIP_FILE = $(BASE_NAME)-$(shell date +%Y%m%d-%H%M%S).zip

.PHONY: zip

zip:
	@echo "📦 Zipping current directory to $(ZIP_FILE)..."
	@zip -r  $(ZIP_FILE) . \
		-x ".git/*" "**/.git/*" \
		-x ".env*" "**/.env*" \
		-x "privateKey.hex" "**/privateKey.hex" \
		-x "kalshi.pem" "**/kalshi.pem" \
		-x "target/*" "**/target/*" \
		-x "node_modules/*" "**/node_modules/*" \
		-x "dist/*" "**/dist/*" \
		-x "$(BASE_NAME)-*.zip"
	@echo "✅ Done! Archive created: $(ZIP_FILE)"


.PHONY: build

build:
	@echo "🛠️  Running code generators..."
	$(MAKE) -C web sqlc && \
	$(MAKE) -C web minify && \
	$(MAKE) -C web templ && \
	$(MAKE) -C web go-vendor && \
	echo "🚀 Deploying with Docker Compose..." && \
	docker compose down
	docker compose up --build -d
	docker compose logs -f