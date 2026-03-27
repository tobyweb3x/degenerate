format:
	@templ fmt .

generate:
	@templ generate

run:
	@tailwindcss -i public/assets/styles/input.css  -o public/assets/styles/output.css --minify
	@templ generate && templ fmt .

tailwind:
	@tailwindcss -i public/assets/styles/input.css  -o public/assets/styles/output.css --watch --minify

.PHONY: format generate run tailwind

