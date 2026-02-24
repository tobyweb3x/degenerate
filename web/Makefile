
run:
	@templ generate --watch  --open-browser=false -v  --cmd="go run ." --proxy="http://localhost:8090" 

tailwind:
	@$(MAKE) -C view tailwind

sqlc:
	@$(MAKE) -C service/repository sqlc
