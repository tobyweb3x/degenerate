
run:
	@templ generate --watch  --open-browser=false -v  --cmd="go run ." --proxy="http://localhost:8090" 

tailwind:
	@$(MAKE) -C view tailwind

minify:
	@$(MAKE) -C view minify

sqlc:
	@$(MAKE) -C service/repository sqlc

templ:
	@$(MAKE) -C view generate

proto-go:
	@$(MAKE) -C ../ proto-go

go-vendor:
	@go mod vendor
