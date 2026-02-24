package frontend

import "embed"

//go:generate npx @tailwindcss/cli -i public/assets/styles/input.css  -o public/assets/styles/output.css 
//go:generate pwd

//go:embed public
var AssetsDir embed.FS
