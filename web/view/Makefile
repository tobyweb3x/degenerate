format:
	@templ fmt .

generate:
	@templ generate

watch:
	@pnpm watch

run:
	@npx @tailwindcss/cli -i public/assets/styles/input.css  -o public/assets/styles/output.css --minify
	@templ generate && templ fmt .

tailwind:
	@npx @tailwindcss/cli -i public/assets/styles/input.css  -o public/assets/styles/output.css --watch --minify

.PHONY: format generate watch run tailwind

